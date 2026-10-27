# Databricks notebook source
# MAGIC %md
# MAGIC # Selected Landing objects to Bronze
# MAGIC Reads fixed Vehicle Positions and reference Static audits, the latest successful Static
# MAGIC audit, and every successful Trip Updates audit started in the last `tu_days` Vancouver
# MAGIC days ending on `tu_end_date` (blank: today), with their payloads; inserts only missing
# MAGIC immutable content and audits. Run twice to inspect
# MAGIC idempotency. This task creates managed development tables.

# COMMAND ----------

# ruff: noqa: F821
import json

from bronze_lib import ingest
from bronze_records import BronzeContractError

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("bronze_schema", "transit_dev_bronze")
dbutils.widgets.text(
    "rt_audit_ref", "audit/capture_attempts/9a50c68781504c3cab4d71e37f61c655.json"
)
dbutils.widgets.text("tu_end_date", "")
dbutils.widgets.text("tu_days", "14")
dbutils.widgets.text(
    "static_audit_ref",
    "audit/schedule_capture_attempts/78fa7b44a0684241af71ba99ab827972.json",
)

try:
    result = ingest(
        spark,
        dbutils,
        dbutils.widgets.get("catalog"),
        dbutils.widgets.get("bronze_schema"),
        dbutils.widgets.get("rt_audit_ref"),
        dbutils.widgets.get("static_audit_ref"),
        dbutils.widgets.get("tu_end_date"),
        dbutils.widgets.get("tu_days"),
    )
except Exception as exc:  # noqa: BLE001 - suppress connector text and credentials.
    failure = {"status": "FAIL", "exception_type": type(exc).__name__}
    if isinstance(exc, BronzeContractError):
        failure["check"] = str(exc)
    print(json.dumps(failure))
    raise RuntimeError(
        "Bronze ingestion failed; inspect task diagnostics without sharing credentials."
    ) from None
print(json.dumps(result, indent=2))
dbutils.notebook.exit(json.dumps(result))
