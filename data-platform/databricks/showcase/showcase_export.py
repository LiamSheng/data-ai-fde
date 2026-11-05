# Databricks notebook source
# MAGIC %md
# MAGIC # Showcase export (branch `showcase`)
# MAGIC Summarises the newest Bronze snapshots into one small public JSON the web page
# MAGIC reads: vehicles on the road, delays over the last hour, active service alerts
# MAGIC and capture health. Reads Bronze only (no Silver/Gold), so it shows what the
# MAGIC Auto Loader job has landed minutes ago. Overwrites `output_uri` every run.

# COMMAND ----------

# ruff: noqa: F821
import json
import re
from datetime import UTC, datetime

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("bronze_schema", "transit_showcase_bronze")
dbutils.widgets.text("output_uri", "gs://transit-realtime-showcase-26/v1/showcase.json")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("bronze_schema")
output_uri = dbutils.widgets.get("output_uri")
for name in (catalog, schema):
    if not re.fullmatch(r"[a-z][a-z0-9_]{0,62}", name):
        raise ValueError("Unsafe SQL identifier")
if not output_uri.startswith("gs://") or not output_uri.endswith(".json"):
    raise ValueError("output_uri must be a gs:// .json object")

S = f"{catalog}.{schema}"
spark.conf.set("spark.sql.session.timeZone", "UTC")
LATE_SECONDS = 300


def rows(sql):
    return [row.asDict() for row in spark.sql(sql).collect()]


def iso(column):
    """A timestamp as UTC ISO text, formatted in SQL so no driver time zone applies."""
    return (
        f"concat(date_format({column}, 'yyyy-MM-dd'), 'T', "
        f"date_format({column}, 'HH:mm:ss'), 'Z')"
    )


def exists(table):
    return spark.catalog.tableExists(f"{S}.{table}")


# COMMAND ----------

# Route names from the newest Static version in Bronze: short name without leading
# zeros, else the long name (as dim_routes does).
if not exists("routes"):
    spark.sql(
        "CREATE OR REPLACE TEMP VIEW showcase_routes AS "
        "SELECT CAST(NULL AS STRING) AS route_id, CAST(NULL AS STRING) AS route_name "
        "WHERE false"
    )
else:
    spark.sql(
        f"""
        CREATE OR REPLACE TEMP VIEW showcase_routes AS
        SELECT route_id,
            coalesce(nullif(regexp_replace(trim(route_short_name), '^0+', ''), ''),
                     nullif(trim(route_long_name), ''), route_id) AS route_name
        FROM {S}.routes
        WHERE static_payload_sha256 = (
            SELECT payload_sha256 FROM {S}.capture_attempts
            WHERE feed_type = 'gtfs_schedule' AND outcome = 'success'
            ORDER BY completed_at_utc DESC, payload_sha256 LIMIT 1)
        """
    )


def latest_snapshot(feed):
    found = rows(
        f"""
        SELECT payload_sha256, {iso("feed_timestamp")} AS feed_timestamp
        FROM {S}.capture_attempts
        WHERE feed_type = '{feed}' AND outcome = 'success'
        ORDER BY completed_at_utc DESC LIMIT 1
        """
    )
    return found[0] if found else None


# COMMAND ----------

vehicles = None
vp = latest_snapshot("vehicle_positions")
if vp:
    counted = f"""
        SELECT coalesce(r.route_name, v.route_id, 'Unassigned') AS route,
               count(DISTINCT v.vehicle_id) AS vehicles
        FROM {S}.vehicle_positions v
        LEFT JOIN showcase_routes r ON v.route_id = r.route_id
        WHERE v.payload_sha256 = '{vp["payload_sha256"]}'
          AND v.has_vehicle AND NOT coalesce(v.is_deleted, false)
        GROUP BY 1
    """
    by_route = rows(counted + " ORDER BY vehicles DESC, route LIMIT 10")
    (total,) = rows(
        f"SELECT sum(vehicles) AS vehicles, count(*) AS routes FROM ({counted})"
    )
    vehicles = {
        "snapshot_at": vp["feed_timestamp"],
        "total": total["vehicles"] or 0,
        "routes_active": total["routes"],
        "top_routes": by_route,
    }

# COMMAND ----------

# Current delay of each running trip at its next stop, over every Trip Updates
# snapshot of the last hour: the same rules as fct_trip_delay_sample (departed trip,
# scheduled stop, prediction 0-5 minutes ahead).
spark.sql(
    f"""
    CREATE OR REPLACE TEMP VIEW showcase_delay_samples AS
    WITH recent AS (
        SELECT payload_sha256 FROM {S}.capture_attempts
        WHERE feed_type = 'trip_updates' AND outcome = 'success'
          AND feed_timestamp >= (
              SELECT max(feed_timestamp) - INTERVAL 1 HOUR FROM {S}.capture_attempts
              WHERE feed_type = 'trip_updates' AND outcome = 'success')
    ),
    stops AS (
        SELECT t.*, row_number() OVER (
            PARTITION BY t.payload_sha256, t.entity_index
            ORDER BY t.stop_sequence, t.update_index) AS stop_rank
        FROM {S}.trip_updates t
        JOIN (SELECT DISTINCT payload_sha256 FROM recent) r USING (payload_sha256)
        WHERE coalesce(t.trip_schedule_relationship, 'SCHEDULED') IN ('SCHEDULED', 'DUPLICATED')
          AND coalesce(t.stop_schedule_relationship, 'SCHEDULED') = 'SCHEDULED'
          AND t.stop_sequence IS NOT NULL AND t.arrival_time IS NOT NULL
          AND t.arrival_delay IS NOT NULL
    )
    SELECT s.payload_sha256, s.feed_timestamp, s.trip_id,
           coalesce(r.route_name, s.route_id, 'Unassigned') AS route,
           s.arrival_delay AS delay_seconds
    FROM stops s LEFT JOIN showcase_routes r ON s.route_id = r.route_id
    WHERE s.stop_rank = 1 AND s.stop_sequence > 1
      AND timestampdiff(SECOND, s.feed_timestamp, s.arrival_time) BETWEEN 0 AND 300
      AND abs(s.arrival_delay) <= 3600
    """
)
(overall,) = rows(
    f"""
    SELECT count(*) AS samples, count(DISTINCT trip_id) AS trips,
           {iso("min(feed_timestamp)")} AS window_from,
           {iso("max(feed_timestamp)")} AS window_to,
           avg(CASE WHEN delay_seconds >= {LATE_SECONDS} THEN 1.0 ELSE 0.0 END) AS late_share,
           percentile_approx(delay_seconds, 0.5) AS median_delay_seconds
    FROM showcase_delay_samples
    """
)
delays = {
    "window_minutes": 60,
    "late_threshold_seconds": LATE_SECONDS,
    "window_from": overall["window_from"],
    "window_to": overall["window_to"],
    "samples": overall["samples"],
    "trips": overall["trips"],
    "late_share": round(float(overall["late_share"] or 0), 3),
    "median_delay_seconds": overall["median_delay_seconds"],
    # Routes with at least 5 trips, so one stuck bus does not top the list.
    "top_routes": [
        {
            **r,
            "avg_delay_seconds": round(float(r["avg_delay_seconds"])),
            "late_share": round(float(r["late_share"]), 3),
        }
        for r in rows(
            f"""
            SELECT route, count(DISTINCT trip_id) AS trips,
                   avg(delay_seconds) AS avg_delay_seconds,
                   avg(CASE WHEN delay_seconds >= {LATE_SECONDS} THEN 1.0 ELSE 0.0 END) AS late_share
            FROM showcase_delay_samples GROUP BY route
            HAVING count(DISTINCT trip_id) >= 5
            ORDER BY avg_delay_seconds DESC LIMIT 10
            """
        )
    ],
}

# COMMAND ----------

alerts = None
sa = latest_snapshot("service_alerts") if exists("service_alerts") else None
if sa:
    names = {
        r["route_id"]: r["route_name"] for r in rows("SELECT * FROM showcase_routes")
    }
    found = rows(
        f"""
        SELECT header_text, effect, cause, informed_entity,
               {iso("array_max(transform(active_period, p -> p.start_utc))")} AS since
        FROM {S}.service_alerts
        WHERE payload_sha256 = '{sa["payload_sha256"]}' AND has_alert
        """
    )
    by_effect = {}
    recent = []
    for alert in found:
        effect = alert["effect"] or "UNKNOWN_EFFECT"
        by_effect[effect] = by_effect.get(effect, 0) + 1
        routes = sorted(
            {
                names.get(e["route_id"], e["route_id"])
                for e in alert["informed_entity"] or []
                if e["route_id"]
            }
        )
        recent.append(
            {
                "header": alert["header_text"],
                "effect": effect,
                "cause": alert["cause"],
                "routes": routes[:8],
                "more_routes": max(len(routes) - 8, 0),
                "since": alert["since"],
            }
        )
    recent.sort(key=lambda a: a["since"] or "", reverse=True)
    alerts = {
        "snapshot_at": sa["feed_timestamp"],
        "total": len(found),
        "by_effect": [
            {"effect": k, "count": v}
            for k, v in sorted(by_effect.items(), key=lambda item: -item[1])
        ],
        "recent": recent[:6],
    }

# COMMAND ----------

feeds = rows(
    f"""
    SELECT feed_type AS feed, count(*) AS attempts,
           sum(CASE WHEN outcome = 'success' THEN 0 ELSE 1 END) AS failed,
           {iso("max(CASE WHEN outcome = 'success' THEN completed_at_utc END)")}
               AS last_success_at,
           {iso("max(feed_timestamp)")} AS newest_data_at
    FROM {S}.capture_attempts
    WHERE started_at_utc >= current_timestamp() - INTERVAL 24 HOURS
    GROUP BY feed_type ORDER BY feed_type
    """
)
quarantined = (
    rows(
        f"""
        SELECT count(DISTINCT audit_ref) AS n FROM {S}.quarantine q
        WHERE event = 'quarantined' AND NOT EXISTS (
            SELECT 1 FROM {S}.quarantine r
            WHERE r.event = 'resolved' AND r.audit_ref = q.audit_ref)
        """
    )[0]["n"]
    if exists("quarantine")
    else 0
)
health = {
    "window_hours": 24,
    "feeds": feeds,
    "quarantined_unresolved": quarantined,
}

# COMMAND ----------

document = {
    "version": 1,
    "generated_at_utc": datetime.now(UTC).isoformat(),
    "source": {"platform": "Databricks", "layer": "Bronze", "schema": S},
    "vehicles": vehicles,
    "delays": delays,
    "alerts": alerts,
    "health": health,
}
text = json.dumps(document, separators=(",", ":"), default=str)
dbutils.fs.put(output_uri, text, overwrite=True)
print(json.dumps({"status": "PASS", "output_uri": output_uri, "bytes": len(text)}))
