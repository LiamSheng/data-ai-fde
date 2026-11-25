"""Bronze table schemas and insert-only, conflict-detecting Delta merges."""

import re
from uuid import uuid4

from bronze_records import (
    fingerprint,
    require,
)

BUCKET = "gs://transit-realtime-landing-26/"
OWNER = "transit-realtime-pipeline"
CONTRACT = "1"
SCHEMAS = {
    "service_alerts": "payload_sha256 STRING, entity_index BIGINT, entity_id STRING, feed_timestamp TIMESTAMP, incrementality STRING, has_alert BOOLEAN, is_deleted BOOLEAN, cause STRING, effect STRING, severity_level STRING, header_text STRING, description_text STRING, url STRING, tts_header_text STRING, cause_detail STRING, effect_detail STRING, informed_entity ARRAY<STRUCT<agency_id: STRING, route_id: STRING, route_type: INT, direction_id: INT, stop_id: STRING, trip: STRUCT<trip_id: STRING, route_id: STRING, direction_id: INT, start_date: STRING, start_time: STRING, schedule_relationship: STRING>>>, active_period ARRAY<STRUCT<start_utc: TIMESTAMP, end_utc: TIMESTAMP>>, raw_entity_json STRING, parser_version STRING, record_sha256 STRING, ingested_at_utc TIMESTAMP",
    "capture_attempts": "attempt_id STRING, poll_cycle_id STRING, feed_type STRING, outcome STRING, http_status INT, payload_ref STRING, payload_sha256 STRING, payload_bytes BIGINT, started_at_utc TIMESTAMP, completed_at_utc TIMESTAMP, feed_timestamp TIMESTAMP, feed_age_seconds DOUBLE, incrementality STRING, entity_count BIGINT, vehicle_count BIGINT, feed_version STRING, audit_ref STRING, audit_sha256 STRING, raw_audit_json STRING, parser_version STRING, record_sha256 STRING, ingested_at_utc TIMESTAMP",
    "vehicle_positions": "payload_sha256 STRING, entity_index BIGINT, entity_id STRING, feed_timestamp TIMESTAMP, incrementality STRING, has_vehicle BOOLEAN, is_deleted BOOLEAN, vehicle_id STRING, trip_id STRING, route_id STRING, vehicle_timestamp TIMESTAMP, latitude DOUBLE, longitude DOUBLE, raw_entity_json STRING, parser_version STRING, record_sha256 STRING, ingested_at_utc TIMESTAMP",
    "trip_updates": "payload_sha256 STRING, entity_index BIGINT, entity_id STRING, feed_timestamp TIMESTAMP, incrementality STRING, has_trip_update BOOLEAN, trip_id STRING, route_id STRING, direction_id INT, start_date STRING, trip_schedule_relationship STRING, trip_timestamp TIMESTAMP, vehicle_id STRING, update_index BIGINT, stop_sequence BIGINT, stop_id STRING, stop_schedule_relationship STRING, arrival_time TIMESTAMP, arrival_delay INT, departure_time TIMESTAMP, departure_delay INT, raw_trip_json STRING, raw_stop_time_update_json STRING, parser_version STRING, record_sha256 STRING, ingested_at_utc TIMESTAMP",
    "routes": "static_payload_sha256 STRING, route_id STRING, agency_id STRING, route_short_name STRING, route_long_name STRING, route_type STRING, raw_route_json STRING, parser_version STRING, record_sha256 STRING, ingested_at_utc TIMESTAMP",
    "calendar": "static_payload_sha256 STRING, service_id STRING, monday STRING, tuesday STRING, wednesday STRING, thursday STRING, friday STRING, saturday STRING, sunday STRING, start_date STRING, end_date STRING, raw_calendar_json STRING, parser_version STRING, record_sha256 STRING, ingested_at_utc TIMESTAMP",
    "calendar_dates": "static_payload_sha256 STRING, service_id STRING, date STRING, exception_type STRING, raw_calendar_date_json STRING, parser_version STRING, record_sha256 STRING, ingested_at_utc TIMESTAMP",
    # Append-only events: a file is quarantined, later perhaps resolved by replay or
    # waived by an operator. failed_check holds the failed check of a quarantined
    # event and the operator's reason of a waived one; it is null on resolved.
    # ingested_at_utc is when the event was first recorded.
    "quarantine": "audit_ref STRING, audit_sha256 STRING, feed_type STRING, event STRING, failed_check STRING, parser_version STRING, record_sha256 STRING, ingested_at_utc TIMESTAMP",
}
KEYS = {
    "service_alerts": ["payload_sha256", "entity_index"],
    "capture_attempts": ["attempt_id"],
    "vehicle_positions": ["payload_sha256", "entity_index"],
    "trip_updates": ["payload_sha256", "entity_index", "update_index"],
    "routes": ["static_payload_sha256", "route_id"],
    "calendar": ["static_payload_sha256", "service_id"],
    "calendar_dates": ["static_payload_sha256", "service_id", "date"],
    "quarantine": ["audit_ref", "audit_sha256", "event", "failed_check"],
}


def identifier(value):
    require(re.fullmatch(r"[a-z][a-z0-9_]{0,62}", value), "Unsafe SQL identifier")
    return value


def merge_condition(keys):
    # Null-safe: a trip without stop updates keys on a null update_index.
    return " AND ".join(f"t.{key} <=> s.{key}" for key in keys)


def merge_records(spark, namespace, name, records):
    from pyspark.sql import functions as F

    table = f"{namespace}.{name}"
    keys = KEYS[name]
    require(
        len(records) == len({tuple(row[key] for key in keys) for row in records}),
        "Input contains duplicate immutable keys",
    )
    if spark.catalog.tableExists(table):
        properties = {
            row[0]: row[1] for row in spark.sql(f"SHOW TBLPROPERTIES {table}").collect()
        }
        require(
            properties.get("transit.owner") == OWNER
            and properties.get("transit.contract_version") == CONTRACT,
            "Existing table is not owned by this ingestion contract",
        )
    else:
        spark.sql(
            f"CREATE TABLE {table} ({SCHEMAS[name]}) USING DELTA TBLPROPERTIES ('transit.owner'='{OWNER}', 'transit.contract_version'='{CONTRACT}')"
        )
    expected = spark.createDataFrame([], SCHEMAS[name]).schema
    require(
        [(field.name, field.dataType) for field in spark.table(table).schema]
        == [(field.name, field.dataType) for field in expected],
        "Existing table schema does not match contract",
    )
    frame = spark.createDataFrame(records, expected)
    if frame.limit(1).count() == 0:
        return {"input_rows": 0, "existing_rows": 0, "inserted_rows": 0}
    source = frame.alias("s")
    target = spark.table(table).alias("t")
    join = [F.col("s." + key).eqNullSafe(F.col("t." + key)) for key in keys]
    matches = source.join(target, join, "inner")
    require(
        matches.where(~F.col("s.record_sha256").eqNullSafe(F.col("t.record_sha256")))
        .limit(1)
        .count()
        == 0,
        "Immutable key collision: existing content differs",
    )
    require(
        matches.groupBy(*[F.col("s." + key) for key in keys])
        .count()
        .where(F.col("count") > 1)
        .limit(1)
        .count()
        == 0,
        "Existing target contains duplicate immutable keys",
    )
    existing = matches.count()
    require(
        existing <= len(records), "Existing target contains duplicate immutable keys"
    )
    temp = "transit_bronze_input_" + uuid4().hex
    frame.createOrReplaceTempView(temp)
    try:
        spark.sql(
            f"MERGE INTO {table} t USING {temp} s ON {merge_condition(keys)} WHEN NOT MATCHED THEN INSERT *"
        )
    finally:
        spark.catalog.dropTempView(temp)
    return {
        "input_rows": len(records),
        "existing_rows": existing,
        "inserted_rows": len(records) - existing,
    }


class SparkTables:
    """Bronze Delta tables in one namespace, written only through merge_records."""

    def __init__(self, spark, namespace):
        self.spark, self.namespace, self.created = spark, namespace, False

    def quarantine_events(self):
        """Every quarantine event; their meaning is bronze_autoloader.quarantine_state.

        Read whole: quarantine holds one or a few rows per bad file, so it stays
        small unless the source contract breaks wholesale.
        """
        table = f"{self.namespace}.quarantine"
        if not self.spark.catalog.tableExists(table):
            return []
        columns = (
            "audit_ref, audit_sha256, feed_type, event, failed_check, ingested_at_utc"
        )
        return [
            row.asDict()
            for row in self.spark.sql(f"SELECT {columns} FROM {table}").collect()
        ]

    def merge(self, name, records):
        # Created on the first write, so a contract error leaves no new schema behind.
        if not self.created:
            self.spark.sql(
                f"CREATE SCHEMA IF NOT EXISTS {self.namespace} COMMENT 'Transit development Bronze records'"
            )
            self.created = True
        return merge_records(self.spark, self.namespace, name, records)


def stamp(records, now):
    for row in records:
        row["record_sha256"] = fingerprint(row)
        row["ingested_at_utc"] = now
    return records
