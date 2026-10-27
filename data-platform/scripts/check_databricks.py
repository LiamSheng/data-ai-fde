"""Run the offline Databricks checks: bundle/dbt checker, then dbt tests on DuckDB.

Local only: needs the dbt project venv (databricks/dbt/.venv), the databricks CLI
and, for replay, captures under data/. Cloud Build's check image excludes
databricks/ and runs scripts/check_ci.py instead.
"""

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DUCKDB_CHECK = "databricks/dbt/dev/duckdb_check.py"
COMMANDS = [
    # Parses and compiles dbt, validates the bundle, runs the ingestion unittests.
    [str(ROOT / "databricks/dbt/.venv/bin/python"), "databricks/check_local.py"],
    [sys.executable, DUCKDB_CHECK, "unit"],
    [sys.executable, DUCKDB_CHECK, "function"],
    [sys.executable, DUCKDB_CHECK, "replay"],
]


def main() -> int:
    for command in COMMANDS:
        print("$ " + " ".join(Path(part).name for part in command), flush=True)
        if subprocess.run(command, cwd=ROOT, check=False).returncode != 0:
            return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
