# Databricks notebook source
# MAGIC %md
# MAGIC # Landing sample read acceptance
# MAGIC Read exactly two audit objects and their referenced payloads through Unity Catalog.
# MAGIC Configure `gtfs-realtime-bindings==3.0.0` in the serverless task environment.
# MAGIC Required parameters: `rt_audit_ref`, `static_audit_ref` (relative object paths).
# MAGIC Source-only verification: no tables, checkpoints, GCS writes, or source HTTP requests.

# COMMAND ----------

# These names are supplied by the Databricks notebook runtime.
# ruff: noqa: F821
import csv
import hashlib
import io
import json
import math
import re
import zipfile
from datetime import UTC, datetime
from importlib.metadata import version

dbutils.widgets.text("rt_audit_ref", "", "Vehicle Positions audit object")
dbutils.widgets.text("static_audit_ref", "", "Static audit object")

BUCKET = "gs://transit-realtime-landing-26/"
MIB = 1024 * 1024
report = {
    "source_read_acceptance": "NOT_RUN",
    "scope": "one vehicle_positions payload and one Static archive",
    "objects": [],
    "checks": [],
    "warnings": [],
    "limitations": [
        "No full-window collection completeness or freshness SLA acceptance.",
        "No proof of historical Static-to-RT version linkage.",
        "No proof of future runtime-principal access or managed-table writes.",
        "Only routes.txt is decompressed and CRC-checked, not every ZIP member.",
    ],
}
stage = "parameters"


class AcceptanceFailure(ValueError):
    pass


def check(name, condition):
    report["checks"].append({"name": name, "status": "PASS" if condition else "FAIL"})
    if not condition:
        raise AcceptanceFailure(name)


def utc(value):
    if not isinstance(value, str):
        raise AcceptanceFailure("missing_or_invalid_utc_timestamp")
    result = datetime.fromisoformat(value)
    if result.tzinfo is None or result.utcoffset().total_seconds() != 0:
        raise AcceptanceFailure("timestamp_must_be_utc")
    return result.astimezone(UTC)


def read_exact(ref, limit):
    """Use UC-aware APIs, with an exact path and a size check before collecting bytes."""
    uri = BUCKET + ref
    entries = dbutils.fs.ls(uri)
    check(ref + ":single_file", len(entries) == 1 and not entries[0].isDir())
    check(ref + ":bounded_size", 0 < entries[0].size <= limit)
    rows = spark.read.format("binaryFile").load(uri).select("length", "content").take(2)
    check(ref + ":single_binary_row", len(rows) == 1)
    payload = bytes(rows[0]["content"])
    check(
        ref + ":read_length",
        len(payload) == rows[0]["length"] == entries[0].size and len(payload) <= limit,
    )
    report["objects"].append(
        {
            "ref": ref,
            "bytes": len(payload),
            "sha256": hashlib.sha256(payload).hexdigest(),
        }
    )
    return payload


def read_audit(ref, prefix, feed_type):
    check(
        "audit_path:" + feed_type,
        re.fullmatch(prefix + r"/[0-9a-f]{32}\.json", ref) is not None,
    )
    audit = json.loads(read_exact(ref, 128 * 1024).decode("utf-8"))
    check("audit_object:" + feed_type, isinstance(audit, dict))
    check(
        "attempt_identity:" + feed_type,
        audit.get("attempt_id") == ref.rsplit("/", 1)[1][:-5],
    )
    status = audit.get("http_status")
    check(
        "successful_audit:" + feed_type,
        audit.get("feed_type") == feed_type
        and audit.get("outcome") == "success"
        and type(status) is int
        and 200 <= status < 300
        and audit.get("error_category") is None,
    )
    check(
        "audit_time_order:" + feed_type,
        utc(audit.get("completed_at_utc")) >= utc(audit.get("started_at_utc")),
    )
    return audit


def read_payload(audit, feed_type, extension, limit):
    ref = audit.get("payload_ref", "")
    digest = audit.get("payload_sha256", "")
    size = audit.get("payload_bytes")
    check(
        "payload_path:" + feed_type,
        re.fullmatch(feed_type + r"/[0-9a-f]{2}/[0-9a-f]{64}\." + extension, ref)
        is not None,
    )
    check(
        "payload_reference:" + feed_type,
        isinstance(digest, str)
        and ref == f"{feed_type}/{digest[:2]}/{digest}.{extension}",
    )
    check("audit_payload_size:" + feed_type, type(size) is int and 0 < size <= limit)
    payload = read_exact(ref, limit)
    check("payload_bytes:" + feed_type, len(payload) == size)
    check("payload_sha256:" + feed_type, hashlib.sha256(payload).hexdigest() == digest)
    return payload


# COMMAND ----------

try:
    rt_ref = dbutils.widgets.get("rt_audit_ref").strip()
    static_ref = dbutils.widgets.get("static_audit_ref").strip()
    check("both_audit_parameters_present", bool(rt_ref and static_ref))

    stage = "dependencies"
    from google.transit import gtfs_realtime_pb2

    report["dependencies"] = {
        "gtfs-realtime-bindings": version("gtfs-realtime-bindings"),
        "protobuf": version("protobuf"),
    }
    check("bindings_version", version("gtfs-realtime-bindings") == "3.0.0")

    stage = "rt_audit_read"
    rt = read_audit(rt_ref, "audit/capture_attempts", "vehicle_positions")
    stage = "rt_payload_read"
    rt_bytes = read_payload(rt, "vehicle_positions", "pb", 4 * MIB)
    stage = "rt_parse"
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(rt_bytes)
    check(
        "protobuf_required_fields",
        feed.IsInitialized() and bool(feed.header.gtfs_realtime_version),
    )
    counts = {
        "entity_count": len(feed.entity),
        "vehicle_count": sum(e.HasField("vehicle") for e in feed.entity),
        "trip_update_count": sum(e.HasField("trip_update") for e in feed.entity),
        "alert_count": sum(e.HasField("alert") for e in feed.entity),
    }
    for name, actual in counts.items():
        if name in rt:
            check("audit_matches:" + name, type(rt[name]) is int and rt[name] == actual)
        else:
            if name == "entity_count":
                check("audit_entity_count_present", False)
            report["warnings"].append("Audit field unavailable: " + name)
    incrementality = gtfs_realtime_pb2.FeedHeader.Incrementality.Name(
        feed.header.incrementality
    )
    check("audit_incrementality", rt.get("incrementality") == incrementality)
    report["rt"] = dict(counts, incrementality=incrementality)
    if incrementality != "FULL_DATASET":
        report["warnings"].append(
            "DIFFERENTIAL feed requires state reconstruction before snapshot counts."
        )

    stage = "freshness"
    check("payload_feed_timestamp_present", feed.header.HasField("timestamp"))
    feed_time = datetime.fromtimestamp(feed.header.timestamp, tz=UTC)
    check("audit_feed_timestamp", feed_time == utc(rt.get("feed_timestamp")))
    age = (utc(rt["completed_at_utc"]) - feed_time).total_seconds()
    recorded_age = rt.get("feed_age_seconds")
    check(
        "audit_age_available",
        type(recorded_age) in (int, float) and math.isfinite(recorded_age),
    )
    check(
        "audit_age_matches_completion",
        math.isclose(age, recorded_age, rel_tol=0, abs_tol=0.001),
    )
    report["freshness"] = {
        "feed_timestamp": feed_time.isoformat(),
        "completed_at_utc": rt["completed_at_utc"],
        "age_at_capture_completion_seconds": age,
        "age_at_notebook_execution_seconds": (
            datetime.now(UTC) - feed_time
        ).total_seconds(),
        "sla_status": "NOT_EVALUATED_NO_THRESHOLD_AGREED",
    }
    if age < 0:
        report["warnings"].append(
            "Negative capture freshness: inspect producer/collector clocks."
        )

    stage = "static_audit_read"
    static = read_audit(static_ref, "audit/schedule_capture_attempts", "gtfs_schedule")
    stage = "static_payload_read"
    static_bytes = read_payload(static, "gtfs_schedule", "zip", 32 * MIB)
    stage = "routes_parse"
    with zipfile.ZipFile(io.BytesIO(static_bytes)) as archive:
        members = archive.infolist()
        names = [entry.filename for entry in members]
        check("unique_zip_member_names", len(names) == len(set(names)))
        check("routes_member_exists", "routes.txt" in names)
        member = archive.getinfo("routes.txt")
        check("routes_member_size", 0 < member.file_size <= 4 * MIB)
        # Reading this member checks its CRC; no extraction to disk or testzip of other members.
        reader = csv.DictReader(io.StringIO(archive.read(member).decode("utf-8-sig")))
        check("routes_id_column", "route_id" in (reader.fieldnames or []))
        routes = []
        for row in reader:
            if len(routes) >= 50_000:
                raise AcceptanceFailure("routes_row_limit_exceeded")
            routes.append(row)
    check("routes_nonempty", bool(routes))
    route_ids = [row.get("route_id") for row in routes]
    check(
        "routes_nonblank_keys",
        all(isinstance(key, str) and key.strip() for key in route_ids),
    )
    check("routes_unique_keys", len(route_ids) == len(set(route_ids)))
    known_routes = set(route_ids)
    report["static"] = {
        "feed_version_from_audit": static.get("feed_version"),
        "route_rows": len(routes),
        "unique_route_ids": len(known_routes),
        "capture_completed_at_utc": static["completed_at_utc"],
        "observed_before_rt_capture": utc(static["completed_at_utc"])
        <= utc(rt["started_at_utc"]),
        "historical_version_linkage": "NOT_PROVEN_BY_SAMPLE",
    }

    stage = "modeling_profile"
    active = [
        e.vehicle for e in feed.entity if e.HasField("vehicle") and not e.is_deleted
    ]
    vehicle_ids = [
        v.vehicle.id for v in active if v.HasField("vehicle") and v.vehicle.id.strip()
    ]
    trip_ids = [
        v.trip.trip_id for v in active if v.HasField("trip") and v.trip.trip_id.strip()
    ]
    rt_routes = [
        v.trip.route_id
        for v in active
        if v.HasField("trip") and v.trip.route_id.strip()
    ]
    matched = sum(route_id in known_routes for route_id in rt_routes)
    distinct_ids = len(set(vehicle_ids))
    missing_vehicles = len(active) - len(vehicle_ids)
    missing_routes = len(active) - len(rt_routes)
    report["modeling_profile"] = {
        "active_vehicle_records": len(active),
        "deleted_entities_excluded": sum(bool(e.is_deleted) for e in feed.entity),
        "missing_vehicle_id_records": missing_vehicles,
        "distinct_nonblank_vehicle_ids": distinct_ids,
        "repeated_vehicle_id_records": len(vehicle_ids) - distinct_ids,
        "missing_trip_id_records": len(active) - len(trip_ids),
        "missing_route_id_records": missing_routes,
        "route_id_coverage": len(rt_routes) / len(active) if active else None,
        "route_matched_records": matched,
        "route_unmatched_records": len(rt_routes) - matched,
        "route_match_rate_among_present_ids": matched / len(rt_routes)
        if rt_routes
        else None,
        "unmatched_distinct_route_count": len(set(rt_routes) - known_routes),
    }
    report["modeling_review_required"] = True
    report["warnings"].append(
        "Static version linkage needs a separate temporal mapping rule."
    )
    if not active or missing_vehicles or missing_routes or len(rt_routes) != matched:
        report["warnings"].append(
            "Review empty/missing/unmatched records before defining Gold vehicle counts."
        )
    if len(vehicle_ids) != distinct_ids:
        report["warnings"].append(
            "Repeated vehicle IDs inside this payload need a business rule; not proof of repeated collection."
        )
    if not report["static"]["observed_before_rt_capture"]:
        report["warnings"].append(
            "Selected Static was captured after RT; historical applicability is unverified."
        )
    check("exactly_four_objects_read", len(report["objects"]) == 4)
    report["source_read_acceptance"] = "PASS"
except Exception as exc:  # noqa: BLE001 - redact connector error text in run output.
    report["source_read_acceptance"] = "FAIL"
    # Do not emit connector exception text, signed URLs, credentials, or raw vehicle data.
    report["failure"] = {"stage": stage, "exception_type": type(exc).__name__}
    if isinstance(exc, AcceptanceFailure):
        report["failure"]["check"] = str(exc)

result_json = json.dumps(report, ensure_ascii=False, indent=2, allow_nan=False)
print(result_json)
print("SOURCE_READ_ACCEPTANCE=" + report["source_read_acceptance"])
if report["source_read_acceptance"] != "PASS":
    raise RuntimeError("SOURCE_READ_ACCEPTANCE_FAILED at " + stage) from None
dbutils.notebook.exit(result_json)
