"""Generate the Genie space definition the bundle deploys (KAN-18).

The space holds the delay sample fact, the route dimension, the alias seed and the
route_delay_summary function (ADR 0001). instructions.md and benchmarks.yml are the
sources; route_delay.geniespace.json is generated from them and the dbt manifest, so
table and function names always match what dbt deploys.
"""

import argparse
import hashlib
import json
from pathlib import Path

import yaml

GENIE = Path(__file__).resolve().parent
ROOT = GENIE.parents[1]
SPACE = GENIE / "route_delay.geniespace.json"
FACT = "fct_trip_delay_sample"
FUNCTION = "route_delay_summary"
ARGUMENTS = ("route", "day_type", "start_hour", "end_hour")
DAY_TYPES = {"weekday", "saturday", "sunday"}


class BenchmarkError(ValueError):
    """A benchmark question that cannot become a checked SQL answer."""


def checked(call):
    """A call the function answers as asked: no hour or day type it would silently miss."""
    if set(call) != set(ARGUMENTS):
        raise BenchmarkError(f"call needs exactly {', '.join(ARGUMENTS)}: {call}")
    if not isinstance(call["route"], str) or not call["route"].strip():
        raise BenchmarkError(f"route must be a name: {call}")
    if call["day_type"] not in DAY_TYPES:
        raise BenchmarkError(f"day_type must be one of {sorted(DAY_TYPES)}: {call}")
    hours = (call["start_hour"], call["end_hour"])
    # Observation hours run 0-23; the window's end hour is excluded, so 24 closes a day.
    if not all(isinstance(h, int) and not isinstance(h, bool) for h in hours) or not (
        0 <= hours[0] < hours[1] <= 24
    ):
        raise BenchmarkError(f"hours must satisfy 0 <= start < end <= 24: {call}")
    return call


def relation(manifest, name):
    """catalog.schema.name of a dbt model or seed."""
    node = next(n for n in manifest["nodes"].values() if n["name"] == name)
    return f"{node['database']}.{node['schema']}.{name}"


def sql_literal(value):
    if isinstance(value, int):
        return str(value)
    return "'" + str(value).replace("'", "''") + "'"


def answer_sql(function, calls):
    return " UNION ALL ".join(
        f"SELECT * FROM {function}("
        + ", ".join(sql_literal(checked(c)[arg]) for arg in ARGUMENTS)
        + ")"
        for c in calls
    )


def stable_id(kind, text):
    """32 lowercase hex characters, as serialized_space requires, fixed by content."""
    return hashlib.md5(f"{kind}:{text}".encode()).hexdigest()


def sorted_unique_by_id(items):
    # Databricks rejects collections that are not already sorted. Ids derive from the
    # text, so a repeated id is a phrasing listed twice.
    ids = [item["id"] for item in items]
    if len(ids) != len(set(ids)):
        raise BenchmarkError("a phrasing is listed twice")
    return sorted(items, key=lambda item: item["id"])


def build_space(manifest, instructions, benchmarks):
    fact = relation(manifest, FACT)
    function = fact.rsplit(".", 1)[0] + "." + FUNCTION
    samples, questions = [], []
    for item in benchmarks["questions"]:
        # A benchmark needs exactly one SQL answer, so a question without one (a
        # ranking or out-of-scope question) stays in benchmarks.yml for a manual run.
        if "expected" in item:
            continue
        if not item.get("calls"):
            raise BenchmarkError(f"needs calls or expected: {item['phrasings']}")
        sql = answer_sql(function, item["calls"])
        for phrasing in item["phrasings"]:
            questions.append(
                {
                    "id": stable_id("benchmark", phrasing),
                    "question": [phrasing],
                    "answer": [{"format": "SQL", "content": [sql]}],
                }
            )
        if item.get("sample"):
            first = item["phrasings"][0]
            samples.append({"id": stable_id("sample", first), "question": [first]})
    return {
        "version": 2,
        "config": {"sample_questions": sorted_unique_by_id(samples)},
        "data_sources": {
            "tables": [
                {"identifier": identifier}
                for identifier in sorted(
                    relation(manifest, name)
                    for name in (FACT, "dim_routes", "route_aliases")
                )
            ]
        },
        "instructions": {
            "text_instructions": [
                {
                    "id": stable_id("instructions", "text"),
                    "content": instructions.splitlines(keepends=True),
                }
            ],
            "sql_functions": [
                {"id": stable_id("function", function), "identifier": function}
            ],
        },
        "benchmarks": {"questions": sorted_unique_by_id(questions)},
    }


def rendered(manifest):
    space = build_space(
        manifest,
        (GENIE / "instructions.md").read_text(),
        yaml.safe_load((GENIE / "benchmarks.yml").read_text()),
    )
    return json.dumps(space, ensure_ascii=False, indent=2) + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--manifest",
        type=Path,
        default=ROOT / "databricks/dbt/target/manifest.json",
        help="dbt manifest from dbt parse or compile",
    )
    parser.add_argument(
        "--check", action="store_true", help="fail if the committed file is stale"
    )
    args = parser.parse_args()
    text = rendered(json.loads(args.manifest.read_text()))
    if not args.check:
        SPACE.write_text(text)
    elif not SPACE.exists() or SPACE.read_text() != text:
        raise SystemExit(
            f"{SPACE.relative_to(ROOT)} is stale: run python databricks/genie/build_space.py"
        )


if __name__ == "__main__":
    main()
