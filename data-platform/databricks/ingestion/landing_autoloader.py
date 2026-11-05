# Databricks notebook source
# MAGIC %md
# MAGIC # Landing audits to Bronze with Auto Loader
# MAGIC `mode=stream` drains every audit Auto Loader has not seen yet (availableNow), one
# MAGIC checkpoint per audit prefix in the schema's `checkpoints` Volume. Contract
# MAGIC failures go to `quarantine`; `mode=replay_quarantine` re-runs the unresolved ones.
# MAGIC Point `landing_root` at a `/Volumes/.../` copy and `bronze_schema` at a scratch
# MAGIC `transit_*_bronze` schema to accept changes without touching the shared tables.

# COMMAND ----------

# ruff: noqa: F821
import json
import re
from datetime import UTC, datetime

import bronze_autoloader
import bronze_lib
import bronze_records
from bronze_lib import SparkTables, identifier
from pyspark import cloudpickle

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("bronze_schema", "transit_dev_bronze")
dbutils.widgets.text("landing_root", bronze_lib.BUCKET)
dbutils.widgets.text("max_files_per_trigger", "50")
dbutils.widgets.text("mode", "stream")
# Showcase: blank reads all of Landing; a UTC instant (2026-11-04T00:00:00) skips older files.
dbutils.widgets.text("modified_after", "")

catalog = identifier(dbutils.widgets.get("catalog"))
schema = identifier(dbutils.widgets.get("bronze_schema"))
landing_root = dbutils.widgets.get("landing_root")
max_files = dbutils.widgets.get("max_files_per_trigger")
mode = dbutils.widgets.get("mode")
modified_after = dbutils.widgets.get("modified_after")
if not (schema.startswith("transit_") and schema.endswith("_bronze")):
    raise ValueError("Use a dedicated transit_*_bronze schema")
if not re.fullmatch(r"(gs://[a-z0-9._-]+/|/Volumes/[\w/-]+/)", landing_root):
    raise ValueError("landing_root must be a gs:// bucket or a /Volumes/ directory")
if not re.fullmatch(r"[1-9][0-9]{0,3}", max_files):
    raise ValueError("max_files_per_trigger must be a positive integer")
if mode not in {"stream", "replay_quarantine"}:
    raise ValueError("mode must be stream or replay_quarantine")
if modified_after and not re.fullmatch(
    r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}", modified_after
):
    raise ValueError("modified_after must be blank or YYYY-MM-DDTHH:MM:SS (UTC)")

namespace = f"{catalog}.{schema}"
checkpoint_root = f"/Volumes/{catalog}/{schema}/checkpoints/landing_autoloader/v1/"

# COMMAND ----------

# The batch function runs where notebook-local modules may not be importable;
# ship them by value with it.
for module in (bronze_autoloader, bronze_lib, bronze_records):
    cloudpickle.register_pickle_by_value(module)

spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {namespace}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {namespace}.checkpoints")

try:
    if mode == "stream":
        bronze_autoloader.run_autoloader(
            spark,
            namespace,
            landing_root,
            checkpoint_root,
            int(max_files),
            modified_after,
        )
        result = {"mode": mode}
    else:
        result = {
            "mode": mode,
            **bronze_autoloader.replay_quarantine(
                bronze_autoloader.SparkAuditLanding(spark, landing_root),
                SparkTables(spark, namespace),
            ),
        }
except Exception as exc:  # noqa: BLE001 - suppress connector text and credentials.
    print(json.dumps({"status": "FAIL", "exception_type": type(exc).__name__}))
    raise RuntimeError(
        "Bronze ingestion failed; inspect task diagnostics without sharing credentials."
    ) from None

# COMMAND ----------

tables = SparkTables(spark, namespace)
attempts = (
    spark.sql(
        f"SELECT feed_type, outcome, count(*) AS n FROM {namespace}.capture_attempts "
        "GROUP BY feed_type, outcome ORDER BY feed_type, outcome"
    ).collect()
    if spark.catalog.tableExists(f"{namespace}.capture_attempts")
    else []
)
result.update(
    {
        "status": "PASS",
        "namespace": namespace,
        "checkpoint_root": checkpoint_root,
        "finished_at_utc": datetime.now(UTC).isoformat(),
        "capture_attempts": [row.asDict() for row in attempts],
        "unresolved_quarantine": len(tables.unresolved_quarantine()),
    }
)
print(json.dumps(result, indent=2))
dbutils.notebook.exit(json.dumps(result))
