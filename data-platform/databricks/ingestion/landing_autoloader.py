# Databricks notebook source
# MAGIC %md
# MAGIC # Landing audits to Bronze with Auto Loader
# MAGIC `mode=stream` drains every audit Auto Loader has not seen yet (availableNow), one
# MAGIC checkpoint per audit prefix in the schema's `checkpoints` Volume. A new checkpoint
# MAGIC starts at `discover_from_utc`: `all` (full backfill) or a UTC time. Contract
# MAGIC failures go to `quarantine`; `mode=replay_quarantine` re-runs the open ones, and
# MAGIC `mode=waive_quarantine` closes the `audit_refs` named (space-separated) with a
# MAGIC `waive_reason`. Runbook: ingestion/QUARANTINE.md.
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

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("bronze_schema", "transit_dev_bronze")
dbutils.widgets.text("landing_root", bronze_lib.BUCKET)
dbutils.widgets.text("max_files_per_trigger", "50")
dbutils.widgets.text("mode", "stream")
dbutils.widgets.text("audit_refs", "")
dbutils.widgets.text("waive_reason", "")
# No default: `all` (full backfill) or a UTC start time must be chosen explicitly.
dbutils.widgets.text("discover_from_utc", "")

catalog = identifier(dbutils.widgets.get("catalog"))
schema = identifier(dbutils.widgets.get("bronze_schema"))
landing_root = dbutils.widgets.get("landing_root")
max_files = dbutils.widgets.get("max_files_per_trigger")
mode = dbutils.widgets.get("mode")
if not (schema.startswith("transit_") and schema.endswith("_bronze")):
    raise ValueError("Use a dedicated transit_*_bronze schema")
if not re.fullmatch(r"(gs://[a-z0-9._-]+/|/Volumes/[\w/-]+/)", landing_root):
    raise ValueError("landing_root must be a gs:// bucket or a /Volumes/ directory")
if not re.fullmatch(r"[1-9][0-9]{0,3}", max_files):
    raise ValueError("max_files_per_trigger must be a positive integer")
if mode not in {"stream", "replay_quarantine", "waive_quarantine"}:
    raise ValueError("mode must be stream, replay_quarantine or waive_quarantine")
# Space-separated: the CLI's --params already splits on commas.
audit_refs = dbutils.widgets.get("audit_refs").split()
if not all(re.fullmatch(bronze_records.AUDIT_PATH, ref) for ref in audit_refs):
    raise ValueError("audit_refs must be audit/<prefix>/<attempt_id>.json paths")
if mode != "waive_quarantine" and audit_refs:
    raise ValueError("audit_refs is only used with mode=waive_quarantine")
discover_from_utc = dbutils.widgets.get("discover_from_utc")
# Validate before any Spark work; the options themselves are applied in discover().
bronze_autoloader.discover_options(discover_from_utc)

namespace = f"{catalog}.{schema}"
checkpoint_root = f"/Volumes/{catalog}/{schema}/checkpoints/landing_autoloader/v3/"

# COMMAND ----------

spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {namespace}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {namespace}.checkpoints")

try:
    if mode == "stream":
        converted = bronze_autoloader.run_autoloader(
            spark,
            namespace,
            landing_root,
            checkpoint_root,
            int(max_files),
            discover_from_utc,
        )
        result = {"mode": mode, "converted_audits": converted}
    elif mode == "replay_quarantine":
        result = {
            "mode": mode,
            **bronze_autoloader.replay_quarantine(
                bronze_autoloader.SparkAuditLanding(spark, landing_root),
                SparkTables(spark, namespace),
            ),
        }
    else:
        result = {
            "mode": mode,
            **bronze_autoloader.waive_quarantine(
                SparkTables(spark, namespace),
                audit_refs,
                dbutils.widgets.get("waive_reason"),
            ),
        }
except (bronze_autoloader.WaiverError, bronze_autoloader.BronzeContractError):
    # Fixed messages without connector text: an operator's waiver mistake, or a
    # contract failure outside one file (an immutable key collision on replay).
    raise
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
        "discover_from_utc": discover_from_utc,
        "finished_at_utc": datetime.now(UTC).isoformat(),
        "capture_attempts": [row.asDict() for row in attempts],
        "quarantine": bronze_autoloader.quarantine_summary(tables),
    }
)
print(json.dumps(result, indent=2))
dbutils.notebook.exit(json.dumps(result))
