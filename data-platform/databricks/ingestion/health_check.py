# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze health check
# MAGIC Runs after the ingestion task. Fails when a realtime feed has no successful
# MAGIC capture in Bronze newer than `max_capture_age_minutes`, or when quarantined files
# MAGIC are open, so the job's failure notification carries the alert
# MAGIC (`bronze_health.py`).

# COMMAND ----------

# ruff: noqa: F821
import json
import re
from datetime import UTC, datetime, timedelta

import bronze_autoloader
from bronze_health import health_problems
from bronze_lib import SparkTables, identifier

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("bronze_schema", "transit_dev_bronze")
dbutils.widgets.text("max_capture_age_minutes", "60")

catalog = identifier(dbutils.widgets.get("catalog"))
schema = identifier(dbutils.widgets.get("bronze_schema"))
max_age = dbutils.widgets.get("max_capture_age_minutes")
if not (schema.startswith("transit_") and schema.endswith("_bronze")):
    raise ValueError("Use a dedicated transit_*_bronze schema")
if not re.fullmatch(r"[1-9][0-9]{0,4}", max_age):
    raise ValueError("max_capture_age_minutes must be a positive integer")
namespace = f"{catalog}.{schema}"

# COMMAND ----------

spark.conf.set("spark.sql.session.timeZone", "UTC")
last_success = {}
if spark.catalog.tableExists(f"{namespace}.capture_attempts"):
    # Epoch microseconds: a collected TIMESTAMP is naive and in the Python
    # process's zone, not necessarily the session's.
    for row in spark.sql(
        f"SELECT feed_type, unix_micros(max(completed_at_utc)) AS last_success "
        f"FROM {namespace}.capture_attempts WHERE outcome = 'success' GROUP BY feed_type"
    ).collect():
        last_success[row["feed_type"]] = datetime.fromtimestamp(
            row["last_success"] / 1_000_000, UTC
        )
quarantine = bronze_autoloader.quarantine_summary(SparkTables(spark, namespace))
now = datetime.now(UTC)
problems = health_problems(
    last_success, quarantine, now, timedelta(minutes=int(max_age))
)

result = {
    "status": "FAIL" if problems else "PASS",
    "namespace": namespace,
    "checked_at_utc": now.isoformat(),
    "last_success_utc": {
        feed: value.isoformat() for feed, value in last_success.items()
    },
    "quarantine": quarantine,
    "problems": problems,
}
print(json.dumps(result, indent=2))
if problems:
    raise RuntimeError("Bronze health check failed: " + "; ".join(problems))
dbutils.notebook.exit(json.dumps(result))
