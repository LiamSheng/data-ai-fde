"""Run this dbt project's SQL on DuckDB, offline, as a fast local approximation.

dbt unit tests and data tests normally execute only on the Databricks SQL
warehouse. This tool compiles the project with dbt (no warehouse connection),
transpiles the compiled Databricks SQL to DuckDB with sqlglot, and runs it:

  unit      run the dbt unit tests (all, or --select NAME ...)
  function  run the route_delay_summary SQL function cases in dev/function_tests.yml
  replay  load the local Static (data/audit) and Trip Updates (data/ops) audits
          with their payloads (data/raw) through the Bronze converters, build
          every model, run the data tests (honouring warn_if / error_if), and
          print a per-snapshot summary of fct_trip_delay_sample (or --sql QUERY)

Databricks remains the source of truth. A pass here is evidence, not
acceptance: dialect differences can hide or invent failures. Known example:
dayofweek() numbers days differently in the two engines, so models use
date_format(..., 'EEE') instead.

Run from the repository root with the root venv (it has the Bronze converter's
protobuf; the dbt venv cannot, see databricks/dbt/README.md):

  .venv/bin/python databricks/dbt/dev/duckdb_check.py unit
  .venv/bin/python databricks/dbt/dev/duckdb_check.py function
  .venv/bin/python databricks/dbt/dev/duckdb_check.py replay
"""

import argparse
import ast
import csv
import datetime as dt
import hashlib
import json
import re
import subprocess
import sys
from decimal import Decimal
from pathlib import Path

import duckdb
import sqlglot
import yaml

ROOT = Path(__file__).resolve().parents[3]
DBT = ROOT / "databricks/dbt"
INGESTION = ROOT / "databricks/ingestion"
GENIE = ROOT / "databricks/genie"
TARGETS = DBT / "target/duckdb_check"
# Sources resolve through env_var defaults that stay unrendered in offline compiles.
BASE_VARS = {"bronze_catalog": "workspace", "bronze_schema": "transit_dev_bronze"}
SPARK_TO_DUCKDB = {"STRING": "VARCHAR", "TIMESTAMP": "TIMESTAMPTZ"}
SEED_TYPES = {"string": "VARCHAR", "date": "DATE", "int": "INTEGER", "bigint": "BIGINT"}


COMPILED = set()


def compile_manifest(dbt_vars):
    """Compile offline with these vars, once per run (never reuse a stale compile)."""
    merged = {**BASE_VARS, **dbt_vars}
    key = hashlib.sha1(json.dumps(merged, sort_keys=True).encode()).hexdigest()[:10]
    target = TARGETS / key
    if key not in COMPILED:
        COMPILED.add(key)
        command = [
            str(DBT / ".venv/bin/dbt"),
            "compile",
            "-q",
            "--project-dir",
            str(DBT),
            "--profiles-dir",
            str(DBT),
            "--target-path",
            str(target),
            "--vars",
            json.dumps(merged),
            "--no-introspect",
            "--no-populate-cache",
            "--indirect-selection",
            "empty",
            "--select",
            "resource_type:model",
            "resource_type:test",
        ]
        process = subprocess.run(command, capture_output=True, text=True, check=False)
        if process.returncode:
            sys.exit(process.stdout + process.stderr)
    return json.loads((target / "manifest.json").read_text())


def compile_inline(code):
    """Compile one Jinja snippet offline (refs, vars, macros) to Databricks SQL."""
    command = [
        str(DBT / ".venv/bin/dbt"),
        "compile",
        "-q",
        "--output",
        "json",
        "--inline",
        code,
        "--project-dir",
        str(DBT),
        "--profiles-dir",
        str(DBT),
        "--target-path",
        str(TARGETS / "inline"),
        "--vars",
        json.dumps(BASE_VARS),
        "--no-introspect",
        "--no-populate-cache",
    ]
    process = subprocess.run(command, capture_output=True, text=True, check=False)
    if process.returncode:
        sys.exit(process.stdout + process.stderr)
    return json.loads(process.stdout[process.stdout.index("{") :])["compiled"]


def bronze_schemas():
    """Bronze DDL from bronze_lib.SCHEMAS, read without importing Spark-side code."""
    tree = ast.parse((INGESTION / "bronze_lib.py").read_text())
    for node in tree.body:
        if isinstance(node, ast.Assign) and node.targets[0].id == "SCHEMAS":
            return ast.literal_eval(node.value)
    raise RuntimeError("SCHEMAS not found in bronze_lib.py")


def duckdb_sql(code):
    return sqlglot.transpile(code, read="databricks", write="duckdb")[0]


def relation(name):
    return name.replace("`", '"')


class Warehouse:
    """An in-memory DuckDB laid out like the dbt target: sources, seeds, model views."""

    def __init__(self, manifest):
        self.manifest = manifest
        self.con = duckdb.connect()
        self.con.execute("SET TimeZone='UTC'")
        # sqlglot leaves Databricks try_to_date as is; models only parse GTFS yyyyMMdd.
        self.con.execute(
            "CREATE MACRO try_to_date(value, format) AS CASE WHEN format = 'yyyyMMdd' "
            "THEN try_strptime(value, '%Y%m%d')::DATE END"
        )
        nodes = list(manifest["nodes"].values()) + list(manifest["sources"].values())
        relations = [n["relation_name"] for n in nodes if n.get("relation_name")]
        for catalog in sorted({r.split(".")[0] for r in relations}):
            self.con.execute(f"ATTACH ':memory:' AS {relation(catalog)}")
        for schema in sorted({".".join(r.split(".")[:2]) for r in relations}):
            self.con.execute(f"CREATE SCHEMA IF NOT EXISTS {relation(schema)}")
        schemas = bronze_schemas()
        for source in manifest["sources"].values():
            columns = ", ".join(
                f"{name} {SPARK_TO_DUCKDB.get(kind, kind)}"
                for name, kind in (
                    c.split(" ") for c in schemas[source["name"]].split(", ")
                )
            )
            self.con.execute(
                f"CREATE TABLE {relation(source['relation_name'])} ({columns})"
            )
        for seed in self.of_type("seed"):
            self.load_seed(seed)
        for model in self.models_in_order():
            self.con.execute(
                f"CREATE VIEW {relation(model['relation_name'])} AS "
                + duckdb_sql(model["compiled_code"])
            )

    def of_type(self, kind):
        return [
            n for n in self.manifest["nodes"].values() if n["resource_type"] == kind
        ]

    def load_seed(self, seed):
        path = DBT / seed["original_file_path"]
        with path.open(newline="") as handle:
            rows = list(csv.DictReader(handle))
            header = rows[0].keys() if rows else next(csv.reader(path.open()))
        types = seed["config"].get("column_types", {})
        columns = ", ".join(
            f"{c} {SEED_TYPES.get(types.get(c, 'string'), 'VARCHAR')}" for c in header
        )
        name = relation(seed["relation_name"])
        self.con.execute(f"CREATE TABLE {name} ({columns})")
        self.insert(name, rows)

    def models_in_order(self):
        models = {n["unique_id"]: n for n in self.of_type("model")}
        ordered, seen = [], set()

        def visit(uid):
            if uid in seen or uid not in models:
                return
            seen.add(uid)
            for parent in models[uid]["depends_on"]["nodes"]:
                visit(parent)
            ordered.append(models[uid])

        for uid in models:
            visit(uid)
        return ordered

    def resolve(self, given_input):
        """ref('m') / source('s', 't') → relation name in this warehouse."""
        ref = re.fullmatch(r"ref\('(\w+)'\)", given_input)
        source = re.fullmatch(r"source\('(\w+)',\s*'(\w+)'\)", given_input)
        if ref:
            return next(
                relation(n["relation_name"])
                for n in self.manifest["nodes"].values()
                if n["name"] == ref[1] and n["resource_type"] in ("model", "seed")
            )
        return next(
            relation(s["relation_name"])
            for s in self.manifest["sources"].values()
            if (s["source_name"], s["name"]) == (source[1], source[2])
        )

    def replace_with_rows(self, name, rows):
        """Swap a relation for a table of the same columns holding fixture rows."""
        columns = self.con.execute(f"DESCRIBE {name}").fetchall()
        kind = self.con.execute(
            "select table_type from information_schema.tables "
            "where table_catalog || '.' || table_schema || '.' || table_name = ?",
            [name.replace('"', "")],
        ).fetchone()[0]
        self.con.execute(f"DROP {'VIEW' if kind == 'VIEW' else 'TABLE'} {name}")
        self.con.execute(
            f"CREATE TABLE {name} ({', '.join(f'{c[0]} {c[1]}' for c in columns)})"
        )
        self.insert(name, rows)

    def insert(self, name, rows):
        for row in rows:
            columns = list(row)
            self.con.execute(
                f"INSERT INTO {name} ({', '.join(columns)}) VALUES ({', '.join('?' * len(columns))})",
                [row[c] for c in columns],
            )

    def query(self, sql):
        cursor = self.con.execute(sql)
        names = [d[0] for d in cursor.description]
        return [dict(zip(names, row)) for row in cursor.fetchall()]


def comparable(value):
    if isinstance(value, dt.datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, dt.date):
        return value.isoformat()
    if isinstance(value, (Decimal, float)) and not isinstance(value, bool):
        return float(value)
    return value


def projected(rows, columns):
    """Rows reduced to the expected columns, order-insensitive, like dbt's comparison."""
    return sorted(
        json.dumps({c: comparable(r.get(c)) for c in columns}, default=str)
        for r in rows
    )


def run_unit_tests(selected):
    failures = 0
    default = compile_manifest({})
    for test in sorted(default["unit_tests"].values(), key=lambda t: t["name"]):
        if selected and test["name"] not in selected:
            continue
        test_vars = (test.get("overrides") or {}).get("vars") or {}
        manifest = compile_manifest(test_vars) if test_vars else default
        warehouse = Warehouse(manifest)
        for given in test["given"]:
            warehouse.replace_with_rows(
                warehouse.resolve(given["input"]), given["rows"]
            )
        model = next(
            n
            for n in manifest["nodes"].values()
            if n["resource_type"] == "model" and n["name"] == test["model"]
        )
        try:
            result = warehouse.query(duckdb_sql(model["compiled_code"]))
        except duckdb.Error as error:
            failures += 1
            print(f"ERROR {test['name']}: {error}")
            continue
        failures += not reported(test["name"], test["expect"]["rows"], result)
    return failures


def reported(name, expected, result):
    """Print PASS / FAIL for rows compared on the expected columns; True on pass."""
    columns = sorted({c for row in expected for c in row})
    want, got = projected(expected, columns), projected(result, columns)
    ok = want == got if expected else not result
    print(("PASS  " if ok else "FAIL  ") + name)
    if not ok:
        print("  expected:", want, "\n  actual:  ", got)
    return ok


FUNCTION = "route_delay_summary"


def function_parameters(header):
    """{name: DEFAULT literal or None} from the compiled CREATE FUNCTION header."""
    header = re.sub(r"'(?:[^']|'')*'", "''", header)  # comments may hold commas
    start = header.index(FUNCTION + "(") + len(FUNCTION) + 1
    end = re.search(r"\)\s*returns\b", header, re.IGNORECASE).start()
    parameters = {}
    for declaration in header[start:end].split(","):
        default = re.search(r"\bdefault\s+(\S+)", declaration, re.IGNORECASE)
        parameters[declaration.split()[0]] = default[1] if default else None
    return parameters


class FunctionBody:
    """The deployed function's body, callable with literal arguments.

    The body names its parameters as route_delay_summary.<name>, as Databricks
    requires when a parameter shares a column name; each is replaced by the given
    SQL literal, or by the DDL's DEFAULT.
    """

    def __init__(self):
        header, self.body = re.split(
            r"^\s*return\s*$",
            compile_inline("{{ route_delay_summary_ddl() }}"),
            1,
            re.MULTILINE | re.IGNORECASE,
        )
        self.parameters = function_parameters(header)

    def called(self, args, label):
        sql = self.body
        for name, default in self.parameters.items():
            value = args.get(name, default)
            if value is None:
                sys.exit(f"{label}: no value for parameter {name}")
            sql = re.sub(rf"\b{FUNCTION}\.{name}\b", str(value), sql)
        return duckdb_sql(sql)


def run_function_tests(selected):
    """Run the deployed function body on DuckDB, one fixture warehouse per case."""
    cases = yaml.safe_load((DBT / "dev/function_tests.yml").read_text())["cases"]
    manifest = compile_manifest({})
    function = FunctionBody()
    failures = 0
    for case in cases:
        if selected and case["name"] not in selected:
            continue
        warehouse = Warehouse(manifest)
        for given in case["given"]:
            warehouse.replace_with_rows(
                warehouse.resolve(given["input"]), given["rows"]
            )
        sql = function.called(case["args"], case["name"])
        try:
            result = warehouse.query(sql)
        except duckdb.Error as error:
            failures += 1
            print(f"ERROR {case['name']}: {error}")
            continue
        failures += not reported(case["name"], [case["expect"]], result)
    return failures


def breaches(result, condition):
    """dbt's warn_if / error_if, e.g. '!=0' or '>=2', applied to the fail_calc result."""
    operator, threshold = re.fullmatch(
        r"\s*(!=|>=|<=|=|>|<)\s*(\d+)\s*", condition
    ).groups()
    value, limit = result or 0, int(threshold)
    return {
        "!=": value != limit,
        "=": value == limit,
        ">=": value >= limit,
        "<=": value <= limit,
        ">": value > limit,
        "<": value < limit,
    }[operator]


def run_benchmark_calls(warehouse):
    """Every Genie benchmark answer runs and names a route found in the replayed data.

    An unresolved route (a typo, or an alias missing from the seed) returns a row with
    a null route_display_name, which Genie would be scored against as correct. A
    question marked insufficient stops testing Not enough evidence once its call has
    Sufficient evidence.
    """
    sys.path.insert(0, str(GENIE))
    from build_space import ARGUMENTS, checked, sql_literal

    function = FunctionBody()
    benchmarks = yaml.safe_load((GENIE / "benchmarks.yml").read_text())["questions"]
    calls, insufficient = set(), set()
    for item in benchmarks:
        for call in item.get("calls", []):
            key = tuple(checked(call)[arg] for arg in ARGUMENTS)
            calls.add(key)
            if item.get("insufficient"):
                insufficient.add(key)
    failing = 0
    for call in sorted(calls, key=str):
        label = f"benchmark call {call}"
        args = {name: sql_literal(value) for name, value in zip(ARGUMENTS, call)}
        try:
            (row,) = warehouse.query(function.called(args, label))
        except duckdb.Error as error:
            print(f"ERROR {label}: {str(error).splitlines()[0]}")
            failing += 1
            continue
        if row["route_display_name"] is None:
            print(f"FAIL  {label}: route not found")
            failing += 1
        elif call in insufficient and row["is_sufficient"]:
            print(f"FAIL  {label}: has Sufficient evidence; pick a quieter window")
            failing += 1
    print(f"Benchmark calls: {len(calls) - failing} resolved, {failing} failed.")
    return failing


def replay(sql):
    sys.path.insert(0, str(INGESTION))
    from bronze_records import (
        audit_record,
        calendar_records,
        fingerprint,
        route_records,
        trip_update_records,
    )

    manifest = compile_manifest({"verify_sample": False})
    warehouse = Warehouse(manifest)

    def load(name, records):
        now = dt.datetime.now(dt.UTC)
        for record in records:
            record["record_sha256"] = fingerprint(record)
            record["ingested_at_utc"] = now
        warehouse.insert(warehouse.resolve(f"source('bronze', '{name}')"), records)

    def load_captures(audits, payloads, feed_type, records):
        """Successful captures of one feed; like Bronze, identical bytes keep one copy.

        records(audit, payload) returns {content table: rows}.
        """
        loaded, seen = 0, set()
        for path in sorted((ROOT / "data" / audits).glob("*.json")):
            raw = path.read_bytes()
            audit = json.loads(raw)
            if audit.get("feed_type") != feed_type or audit.get("outcome") != "success":
                continue
            # Landing keeps every audit under audit/; locally Trip Updates sit in ops/.
            ref = f"audit/{Path(audits).name}/{path.name}"
            _, attempt = audit_record(ref, raw, feed_type)
            if audit["payload_sha256"] not in seen:
                payload = (ROOT / "data" / payloads / audit["payload_ref"]).read_bytes()
                for table, rows in records(audit, payload).items():
                    load(table, rows)
            seen.add(audit["payload_sha256"])
            load("capture_attempts", [attempt])
            loaded += 1
        return loaded

    statics = load_captures(
        "audit/schedule_capture_attempts",
        "raw",
        "gtfs_schedule",
        lambda audit, payload: {
            "routes": route_records(audit, payload),
            **calendar_records(audit, payload),
        },
    )
    print(f"Loaded {statics} successful Static captures from data/.")
    loaded = load_captures(
        "ops/capture_attempts",
        "raw/gtfs_rt",
        "trip_updates",
        lambda audit, payload: {"trip_updates": trip_update_records(audit, payload)},
    )
    print(f"Loaded {loaded} successful Trip Updates captures from data/.")
    failing = 0
    for test in warehouse.of_type("test"):
        if not test.get("compiled_code"):
            continue
        config = test["config"]
        try:
            result = warehouse.query(
                f"select {duckdb_sql(config['fail_calc'])} as n "
                f"from ({duckdb_sql(test['compiled_code'])})"
            )[0]["n"]
        except (duckdb.Error, sqlglot.errors.SqlglotError) as error:
            print(f"ERROR data test {test['name']}: {str(error).splitlines()[0]}")
            failing += 1
            continue
        if breaches(result, config["error_if"]):
            failing += 1
            print(f"FAIL  data test {test['name']}: {result}")
        elif breaches(result, config["warn_if"]):
            print(f"WARN  data test {test['name']}: {result}")
    print(
        f"Data tests: {len(warehouse.of_type('test')) - failing} passed, {failing} failed or errored."
    )
    failing += run_benchmark_calls(warehouse)
    summary = (
        sql
        or """
        select
            left(payload_sha256, 8) as snapshot, min(service_date) as service_date,
            any_value(day_type) as day_type, count(*) as samples,
            count(distinct trip_id) as trips,
            round(100.0 * avg(is_late::int), 1) as pct_late,
            round(100.0 * avg((current_delay_seconds = 0)::int), 1) as pct_zero_delay,
            sum(is_suspect_delay::int) as suspect,
            min(prediction_lead_seconds) as min_lead, max(prediction_lead_seconds) as max_lead
        from workspace.transit_dev_gold.fct_trip_delay_sample
        group by payload_sha256 order by snapshot"""
    )
    rows = warehouse.query(summary)
    if rows:
        print("\t".join(rows[0]))
        for row in rows:
            print("\t".join(str(comparable(v)) for v in row.values()))
    return failing


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    commands = parser.add_subparsers(dest="command", required=True)
    unit = commands.add_parser("unit", help="run dbt unit tests on DuckDB")
    unit.add_argument("--select", nargs="*", default=[], help="unit test names")
    function = commands.add_parser(
        "function", help="run the route_delay_summary cases on DuckDB"
    )
    function.add_argument("--select", nargs="*", default=[], help="case names")
    replay_parser = commands.add_parser(
        "replay", help="replay local Trip Updates captures"
    )
    replay_parser.add_argument("--sql", help="query to print instead of the summary")
    args = parser.parse_args()
    if args.command == "unit":
        failures = run_unit_tests(set(args.select))
    elif args.command == "function":
        failures = run_function_tests(set(args.select))
    else:
        failures = replay(args.sql)
    raise SystemExit(1 if failures else 0)


if __name__ == "__main__":
    main()
