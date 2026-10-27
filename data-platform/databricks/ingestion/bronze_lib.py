"""UC-aware bounded ingestion with insert-only, conflict-detecting Delta merges."""

import json
import re
from datetime import UTC, date, datetime, time, timedelta
from uuid import uuid4
from zoneinfo import ZoneInfo

from bronze_records import (
    BronzeContractError,
    audit_record,
    calendar_records,
    digest,
    fingerprint,
    require,
    route_records,
    trip_update_records,
    utc,
    vehicle_records,
    verified_feed,
)

BUCKET = "gs://transit-realtime-landing-26/"
OWNER = "transit-realtime-pipeline"
CONTRACT = "1"
SCHEMAS = {
    "capture_attempts": "attempt_id STRING, poll_cycle_id STRING, feed_type STRING, outcome STRING, http_status INT, payload_ref STRING, payload_sha256 STRING, payload_bytes BIGINT, started_at_utc TIMESTAMP, completed_at_utc TIMESTAMP, feed_timestamp TIMESTAMP, feed_age_seconds DOUBLE, incrementality STRING, entity_count BIGINT, vehicle_count BIGINT, feed_version STRING, audit_ref STRING, audit_sha256 STRING, raw_audit_json STRING, parser_version STRING, record_sha256 STRING, ingested_at_utc TIMESTAMP",
    "vehicle_positions": "payload_sha256 STRING, entity_index BIGINT, entity_id STRING, feed_timestamp TIMESTAMP, incrementality STRING, has_vehicle BOOLEAN, is_deleted BOOLEAN, vehicle_id STRING, trip_id STRING, route_id STRING, vehicle_timestamp TIMESTAMP, latitude DOUBLE, longitude DOUBLE, raw_entity_json STRING, parser_version STRING, record_sha256 STRING, ingested_at_utc TIMESTAMP",
    "trip_updates": "payload_sha256 STRING, entity_index BIGINT, entity_id STRING, feed_timestamp TIMESTAMP, incrementality STRING, has_trip_update BOOLEAN, trip_id STRING, route_id STRING, direction_id INT, start_date STRING, trip_schedule_relationship STRING, trip_timestamp TIMESTAMP, vehicle_id STRING, update_index BIGINT, stop_sequence BIGINT, stop_id STRING, stop_schedule_relationship STRING, arrival_time TIMESTAMP, arrival_delay INT, departure_time TIMESTAMP, departure_delay INT, raw_trip_json STRING, raw_stop_time_update_json STRING, parser_version STRING, record_sha256 STRING, ingested_at_utc TIMESTAMP",
    "routes": "static_payload_sha256 STRING, route_id STRING, agency_id STRING, route_short_name STRING, route_long_name STRING, route_type STRING, raw_route_json STRING, parser_version STRING, record_sha256 STRING, ingested_at_utc TIMESTAMP",
    "calendar": "static_payload_sha256 STRING, service_id STRING, monday STRING, tuesday STRING, wednesday STRING, thursday STRING, friday STRING, saturday STRING, sunday STRING, start_date STRING, end_date STRING, raw_calendar_json STRING, parser_version STRING, record_sha256 STRING, ingested_at_utc TIMESTAMP",
    "calendar_dates": "static_payload_sha256 STRING, service_id STRING, date STRING, exception_type STRING, raw_calendar_date_json STRING, parser_version STRING, record_sha256 STRING, ingested_at_utc TIMESTAMP",
}
KEYS = {
    "capture_attempts": ["attempt_id"],
    "vehicle_positions": ["payload_sha256", "entity_index"],
    "trip_updates": ["payload_sha256", "entity_index", "update_index"],
    "routes": ["static_payload_sha256", "route_id"],
    "calendar": ["static_payload_sha256", "service_id"],
    "calendar_dates": ["static_payload_sha256", "service_id", "date"],
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


VANCOUVER = ZoneInfo("America/Vancouver")
AUDIT_LIMIT = 128 * 1024
TRIP_UPDATE_AUDIT = r"audit/capture_attempts/[0-9a-f]{32}\.json"
STATIC_AUDIT = r"audit/schedule_capture_attempts/[0-9a-f]{32}\.json"
TRIP_UPDATE_PAYLOAD = r"trip_updates/[0-9a-f]{2}/[0-9a-f]{64}\.pb"
TRIP_UPDATE_LIMIT = 16 * 1024 * 1024
# Payloads parsed and merged together; bounds driver memory, not correctness.
SNAPSHOTS_PER_MERGE = 20


class SparkLanding:
    """The Landing bucket read through Spark; refs are paths inside BUCKET."""

    def __init__(self, spark, dbutils):
        self.spark, self.dbutils = spark, dbutils

    def list(self, prefix, modified_after, modified_before):
        lower, upper = (
            int(value.timestamp() * 1000) for value in (modified_after, modified_before)
        )
        return sorted(
            entry.path.removeprefix(BUCKET)
            for entry in self.dbutils.fs.ls(BUCKET + prefix)
            if not entry.isDir() and lower <= entry.modificationTime < upper
        )

    def read_many(self, refs, limit, pattern):
        for ref in refs:
            require(
                re.fullmatch(pattern, ref),
                "Object path outside the selected production prefix",
            )
        found = {}
        for offset in range(0, len(refs), 500):
            paths = [BUCKET + ref for ref in refs[offset : offset + 500]]
            for row in (
                self.spark.read.format("binaryFile")
                .load(paths)
                .select("path", "length", "content")
                .collect()
            ):
                payload = bytes(row["content"])
                require(
                    0 < len(payload) == row["length"] <= limit,
                    "Missing or oversized sample file",
                )
                found[row["path"].removeprefix(BUCKET)] = payload
        require(set(found) == set(refs), "Expected one exact object per path")
        return found


class SparkTables:
    """Bronze Delta tables in one namespace, written only through merge_records."""

    def __init__(self, spark, namespace):
        self.spark, self.namespace, self.created = spark, namespace, False

    def existing_attempt_ids(self):
        table = f"{self.namespace}.capture_attempts"
        if not self.spark.catalog.tableExists(table):
            return set()
        return {
            row[0] for row in self.spark.table(table).select("attempt_id").collect()
        }

    def merge(self, name, records):
        # Created on the first write, so a contract error leaves no new schema behind.
        if not self.created:
            self.spark.sql(
                f"CREATE SCHEMA IF NOT EXISTS {self.namespace} COMMENT 'Transit development Bronze records'"
            )
            self.created = True
        return merge_records(self.spark, self.namespace, name, records)


def trip_update_range(end_date, days, now):
    """Job parameters to Vancouver dates: `days` days ending on end_date (default today)."""
    require(
        re.fullmatch(r"[1-9][0-9]{0,3}", days), "tu_days must be a positive integer"
    )
    try:
        end = (
            date.fromisoformat(end_date)
            if end_date
            else now.astimezone(VANCOUVER).date()
        )
    except ValueError:
        raise BronzeContractError("tu_end_date must be YYYY-MM-DD") from None
    return end - timedelta(days=int(days) - 1), end


def local_day_bounds(start, end):
    """UTC instants bounding Vancouver calendar days start..end, both inclusive."""
    require(start <= end, "Date range start is after its end")
    lower = datetime.combine(start, time(), VANCOUVER)
    upper = datetime.combine(end + timedelta(days=1), time(), VANCOUVER)
    return lower.astimezone(UTC), upper.astimezone(UTC)


def trip_update_audits(landing, start, end):
    """Successful Trip Updates audits whose attempt started within the date range."""
    lower, upper = local_day_bounds(start, end)
    # An audit is written seconds after its attempt; the day of slack on the
    # object modification time only bounds how many audits are read.
    refs = landing.list(
        "audit/capture_attempts/", lower - timedelta(days=1), upper + timedelta(days=1)
    )
    raws = landing.read_many(refs, AUDIT_LIMIT, TRIP_UPDATE_AUDIT)
    selected = []
    for ref in refs:
        try:
            audit = json.loads(raws[ref])
        except ValueError:
            raise BronzeContractError("Unreadable audit") from None
        require(isinstance(audit, dict), "Unreadable audit")
        if (
            audit.get("feed_type") != "trip_updates"
            or audit.get("outcome") != "success"
        ):
            continue
        require(isinstance(audit.get("started_at_utc"), str), "UTC timestamp required")
        if lower <= utc(audit["started_at_utc"]) < upper:
            selected.append(audit_record(ref, raws[ref], "trip_updates"))
    require(selected, "No successful Trip Updates audit in the date range")
    return selected


def latest_static_audit_ref(landing, now):
    """Ref of the successful Static audit whose attempt completed last.

    Ties go to the smallest payload hash, the order int_latest_static_version uses.
    """
    # Static is captured once a day, so reading every audit stays small; the
    # modification-time bounds select all of them.
    refs = landing.list(
        "audit/schedule_capture_attempts/",
        datetime(1970, 1, 1, tzinfo=UTC),
        now + timedelta(days=1),
    )
    raws = landing.read_many(refs, AUDIT_LIMIT, STATIC_AUDIT)
    candidates = []
    for ref in refs:
        try:
            audit = json.loads(raws[ref])
        except ValueError:
            raise BronzeContractError("Unreadable audit") from None
        require(isinstance(audit, dict), "Unreadable audit")
        if (
            audit.get("feed_type") != "gtfs_schedule"
            or audit.get("outcome") != "success"
        ):
            continue
        require(
            isinstance(audit.get("completed_at_utc"), str), "UTC timestamp required"
        )
        candidates.append(
            (utc(audit["completed_at_utc"]), audit.get("payload_sha256") or "", ref)
        )
    require(candidates, "No successful Static audit")
    _, _, ref = min(candidates, key=lambda c: (-c[0].timestamp(), c[1]))
    return ref


def stamp(records, now):
    for row in records:
        row["record_sha256"] = fingerprint(row)
        row["ingested_at_utc"] = now
    return records


def load(landing, tables, rt_ref, static_ref, tu_start, tu_end, now=None):
    """Fixed VP and reference Static objects, the latest Static capture and every
    Trip Updates snapshot in the date range.

    Every new object is read and validated before the first table write, so a
    contract error leaves the tables untouched. Audits are published last: dbt
    only exposes content joined to a successful audit.
    """
    now = now or datetime.now(UTC)
    # The reference version keeps the VP slice's routes; the latest feeds dim_routes.
    static_refs = dict.fromkeys([static_ref, latest_static_audit_ref(landing, now)])
    fixed = [
        (rt_ref, "vehicle_positions", "capture_attempts", "pb", 4 * 1024 * 1024),
        *(
            (ref, "gtfs_schedule", "schedule_capture_attempts", "zip", 32 * 1024 * 1024)
            for ref in static_refs
        ),
    ]
    audits, payloads, manifest = [], {}, []
    for ref, feed_type, audit_prefix, extension, limit in fixed:
        pattern = r"audit/" + audit_prefix + r"/[0-9a-f]{32}\.json"
        raw = landing.read_many([ref], AUDIT_LIMIT, pattern)[ref]
        audit, record = audit_record(ref, raw, feed_type)
        payload_ref = audit["payload_ref"]
        payload = landing.read_many(
            [payload_ref],
            limit,
            feed_type + r"/[0-9a-f]{2}/[0-9a-f]{64}\." + extension,
        )[payload_ref]
        audits.append(record)
        payloads.setdefault(feed_type, {})[audit["payload_sha256"]] = (audit, payload)
        manifest.extend(
            [
                {"ref": ref, "sha256": digest(raw), "bytes": len(raw)},
                {"ref": payload_ref, "sha256": digest(payload), "bytes": len(payload)},
            ]
        )
    (vehicle_capture,) = payloads["vehicle_positions"].values()
    vehicles = stamp(vehicle_records(*vehicle_capture), now)
    # Two Static attempts of the same version share one set of routes and calendars.
    statics = payloads["gtfs_schedule"].values()
    routes = stamp([row for capture in statics for row in route_records(*capture)], now)
    calendars = [calendar_records(*capture) for capture in statics]
    calendar_tables = {
        name: stamp([row for records in calendars for row in records[name]], now)
        for name in ("calendar", "calendar_dates")
    }

    selected = trip_update_audits(landing, tu_start, tu_end)
    known = tables.existing_attempt_ids()
    # A snapshot is a distinct payload: identical bytes captured twice load once.
    # Attempts already in Bronze were validated when they were first loaded.
    new = {}
    for audit, _ in selected:
        if audit["attempt_id"] not in known:
            new.setdefault(audit["payload_sha256"], []).append(audit)
    chunks = [
        sorted(new)[offset : offset + SNAPSHOTS_PER_MERGE]
        for offset in range(0, len(new), SNAPSHOTS_PER_MERGE)
    ]

    def snapshot_payloads(chunk):
        refs = {sha: new[sha][0]["payload_ref"] for sha in chunk}
        found = landing.read_many(
            list(refs.values()), TRIP_UPDATE_LIMIT, TRIP_UPDATE_PAYLOAD
        )
        return {sha: found[ref] for sha, ref in refs.items()}

    for chunk in chunks:
        for sha, payload in snapshot_payloads(chunk).items():
            for audit in new[sha]:
                verified_feed(
                    audit,
                    payload,
                    "trip_updates",
                    required_counts=("trip_update_count",),
                )

    results = {"vehicle_positions": tables.merge("vehicle_positions", vehicles)}
    totals = {"input_rows": 0, "existing_rows": 0, "inserted_rows": 0}
    for chunk in chunks:
        records = [
            row
            for sha, payload in snapshot_payloads(chunk).items()
            for row in trip_update_records(new[sha][0], payload)
        ]
        for key, value in tables.merge("trip_updates", stamp(records, now)).items():
            totals[key] += value
    results["trip_updates"] = totals
    results["routes"] = tables.merge("routes", routes)
    for name, records in calendar_tables.items():
        results[name] = tables.merge(name, records)
    results["capture_attempts"] = tables.merge(
        "capture_attempts", stamp([*audits, *(row for _, row in selected)], now)
    )
    return {
        "status": "PASS",
        "objects": manifest,
        "trip_update_range": {
            "start_date": tu_start.isoformat(),
            "end_date": tu_end.isoformat(),
            "audits": len(selected),
            "snapshots": len({audit["payload_sha256"] for audit, _ in selected}),
            "new_audits": sum(len(group) for group in new.values()),
            "new_snapshots": len(new),
        },
        "tables": results,
        "parser_version": CONTRACT,
        "static_linkage_status": "unverified_reference",
    }


def ingest(spark, dbutils, catalog, schema, rt_ref, static_ref, tu_end_date, tu_days):
    identifier(catalog)
    identifier(schema)
    require(
        schema.startswith("transit_") and schema.endswith("_bronze"),
        "Use a dedicated transit_*_bronze development schema",
    )
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    namespace = f"{catalog}.{schema}"
    tu_start, tu_end = trip_update_range(tu_end_date, tu_days, datetime.now(UTC))
    result = load(
        SparkLanding(spark, dbutils),
        SparkTables(spark, namespace),
        rt_ref,
        static_ref,
        tu_start,
        tu_end,
    )
    return {"namespace": namespace, **result}
