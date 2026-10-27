"""Check project parsing, SQL syntax and bundle shape without cloud authentication."""

import json
import os
import re
import subprocess
from collections import Counter
from pathlib import Path

import jsonschema
import sqlglot
import yaml

ROOT = Path(__file__).resolve().parents[1]
DBT = ROOT / "databricks/dbt"


def run(command, output="stdout"):
    process = subprocess.run(
        command,
        cwd=ROOT,
        env=dict(os.environ, DBT_SEND_ANONYMOUS_USAGE_STATS="false"),
        capture_output=True,
        text=True,
        check=False,
    )
    if process.returncode:
        print(process.stdout)
        print(process.stderr)
        raise SystemExit(process.returncode)
    return getattr(process, output)


run(
    [
        str(DBT / ".venv/bin/dbt"),
        "parse",
        "--project-dir",
        str(DBT),
        "--profiles-dir",
        str(DBT),
        "--no-partial-parse",
    ]
)
run(
    [
        str(DBT / ".venv/bin/dbt"),
        "compile",
        "--project-dir",
        str(DBT),
        "--profiles-dir",
        str(DBT),
        "--no-introspect",
        "--no-populate-cache",
        "--indirect-selection",
        "empty",
        "--select",
        "resource_type:model",
        "resource_type:test",
    ]
)
manifest = json.loads((DBT / "target/manifest.json").read_text())
schema = json.loads(run(["databricks", "bundle", "schema"]))


def portable_patterns(value):
    # CLI uses Go Unicode regex classes. Our substitution identifiers are ASCII;
    # translate those classes for Python's JSON Schema checker without changing fields.
    if isinstance(value, dict):
        for key, item in value.items():
            if key == "pattern" and isinstance(item, str):
                value[key] = (
                    item.replace(r"[\p{L}\p{N}]", "[A-Za-z0-9]")
                    .replace(r"\p{L}", "[A-Za-z]")
                    .replace(r"\p{N}", "[0-9]")
                )
            else:
                portable_patterns(item)
    elif isinstance(value, list):
        for item in value:
            portable_patterns(item)


portable_patterns(schema)
bundle = yaml.safe_load((ROOT / "databricks/databricks.yml").read_text())
bundle["resources"] = {}
for resource_file in sorted((ROOT / "databricks/resources").glob("*.yml")):
    for kind, items in yaml.safe_load(resource_file.read_text())["resources"].items():
        bundle["resources"].setdefault(kind, {}).update(items)
jsonschema.Draft202012Validator(schema).validate(bundle)

# The Genie space deploys the generated file; regenerate it from its sources and the
# manifest just compiled, so table and function names match what dbt deploys.
space = bundle["resources"]["genie_spaces"]["route_delay"]
if (ROOT / "databricks/resources" / space["file_path"]).resolve() != (
    ROOT / "databricks/genie/route_delay.geniespace.json"
):
    raise SystemExit("Genie space must deploy genie/route_delay.geniespace.json")
run(
    [
        str(ROOT / ".venv/bin/python"),
        "databricks/genie/build_space.py",
        "--check",
        "--manifest",
        str(DBT / "target/manifest.json"),
    ]
)
jobs = bundle["resources"]["jobs"]
if (
    set(jobs) != {"transit_slice"}
    or "schedule" in jobs["transit_slice"]
    or "trigger" in jobs["transit_slice"]
):
    raise SystemExit("Unexpected scheduled resource")
for task in jobs["transit_slice"]["tasks"]:
    if (
        task.get("max_retries") != 0
        or task.get("disable_auto_optimization") is not True
    ):
        raise SystemExit("Retry policy differs from bounded manual acceptance")
ingestion = next(
    task
    for task in jobs["transit_slice"]["tasks"]
    if task["task_key"] == "landing_to_bronze"
)
widgets = set(
    re.findall(
        r'dbutils\.widgets\.text\(\s*"(\w+)"',
        (ROOT / "databricks/ingestion/landing_to_bronze.py").read_text(),
    )
)
parameters = ingestion["notebook_task"]["base_parameters"]
if set(parameters) != widgets:
    raise SystemExit("Job parameters differ from notebook widgets")
for value in parameters.values():
    for variable in re.findall(r"\$\{var\.(\w+)\}", value):
        if variable not in bundle["variables"]:
            raise SystemExit("Job parameter uses an undefined variable: " + variable)

# route_delay_summary reads the fact and dimension, so it deploys after dbt build.
dbt_commands = [
    command.split(" --vars")[0]
    for task in jobs["transit_slice"]["tasks"]
    if "dbt_task" in task
    for command in task["dbt_task"]["commands"]
]
deploy = "dbt run-operation deploy_route_delay_summary"
if deploy not in dbt_commands or dbt_commands.index(deploy) < dbt_commands.index(
    "dbt build"
):
    raise SystemExit("Job must deploy route_delay_summary after dbt build")
if "macro.transit_transformations.deploy_route_delay_summary" not in manifest["macros"]:
    raise SystemExit("deploy_route_delay_summary macro is missing")

syntax_nodes = [
    node
    for node in manifest["nodes"].values()
    if node["resource_type"] in {"model", "test"}
]
for node in syntax_nodes:
    # Core/adapter compiled SQL, without warehouse introspection or materialization.
    if not node.get("compiled") or not node.get("compiled_code"):
        raise SystemExit("Node was not compiled offline: " + node["name"])
    sqlglot.parse_one(node["compiled_code"], read="databricks")

# Bronze keeps source text as published. Databricks (ANSI) fails the whole build on
# one malformed value, which DuckDB replay never shows, so models reading Bronze parse
# with try_cast / try_to_date / try_to_timestamp and leave bad values null for tests.
strict_parse = re.compile(r"(?<!\w)(cast|to_date|to_timestamp)\s*\(", re.IGNORECASE)
for node in syntax_nodes:
    reads_bronze = any(
        dependency.startswith("source.") for dependency in node["depends_on"]["nodes"]
    )
    code = re.sub(r"--[^\n]*", "", node["raw_code"])
    if reads_bronze and strict_parse.search(code):
        raise SystemExit(
            "Use try_cast / try_to_date / try_to_timestamp on Bronze text: "
            + node["name"]
        )


def unittest_count(directory):
    """Run a root-venv unittest suite; the number of tests it ran."""
    report = run(
        [
            str(ROOT / ".venv/bin/python"),
            "-m",
            "unittest",
            "discover",
            "-s",
            directory,
            "-v",
        ],
        output="stderr",  # unittest reports on stderr
    )
    count = re.search(r"Ran (\d+) tests", report)
    if not count:
        raise SystemExit("Test count missing from unittest output: " + directory)
    return int(count[1])


bronze_count = unittest_count("databricks/ingestion/tests")
genie_count = unittest_count("databricks/genie/tests")
print(
    json.dumps(
        {
            "status": "PASS",
            "dbt_core": manifest["metadata"]["dbt_version"],
            "dbt_nodes": dict(
                Counter(node["resource_type"] for node in manifest["nodes"].values())
            ),
            "sources": len(manifest["sources"]),
            "unit_test_definitions": len(manifest.get("unit_tests", {})),
            "dbt_offline_compile": "PASS (no introspection or adapter cache population)",
            "compiled_sql_nodes_syntax_checked": len(syntax_nodes),
            "bronze_local_checks": bronze_count,
            "genie_space_checks": genie_count,
            "bundle_schema": "installed CLI schema with ASCII substitution identifiers",
            "cloud_execution": "NOT_RUN",
        },
        indent=2,
    )
)
