"""Bounded, deterministic conversion of selected Landing objects to Bronze records."""

import csv
import hashlib
import io
import json
import math
import re
import zipfile
from datetime import UTC, datetime

from google.protobuf.json_format import MessageToDict
from google.transit import gtfs_realtime_pb2

PARSER_VERSION = "1"


class BronzeContractError(ValueError):
    """A fixed-message source-contract failure safe to include in task output."""


def require(condition, message):
    if not condition:
        raise BronzeContractError(message)


def utc(value):
    parsed = datetime.fromisoformat(value)
    require(
        parsed.tzinfo is not None and parsed.utcoffset().total_seconds() == 0,
        "UTC timestamp required",
    )
    return parsed.astimezone(UTC)


def canonical(value):
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
        default=lambda item: item.isoformat(),
    )


def digest(payload):
    return hashlib.sha256(payload).hexdigest()


def fingerprint(record):
    # Ingestion time is first-write metadata, not part of immutable source identity.
    return digest(
        canonical(
            {
                key: value
                for key, value in record.items()
                if key not in {"record_sha256", "ingested_at_utc"}
            }
        ).encode()
    )


def audit_record(ref, payload, feed_type):
    prefix = (
        "schedule_capture_attempts"
        if feed_type == "gtfs_schedule"
        else "capture_attempts"
    )
    require(
        re.fullmatch(r"audit/" + prefix + r"/[0-9a-f]{32}\.json", ref),
        "Invalid audit path",
    )
    audit = json.loads(payload)
    require(
        audit.get("attempt_id") == ref.rsplit("/", 1)[1][:-5], "Audit identity mismatch"
    )
    require(
        audit.get("feed_type") == feed_type and audit.get("outcome") == "success",
        "Select a successful audit for this slice",
    )
    require(
        type(audit.get("http_status")) is int and 200 <= audit["http_status"] < 300,
        "Expected HTTP success",
    )
    require(
        audit.get("error_category") is None, "Successful audit has an error category"
    )
    started, completed = utc(audit["started_at_utc"]), utc(audit["completed_at_utc"])
    require(completed >= started, "Audit time order is invalid")
    row = {
        "attempt_id": audit["attempt_id"],
        "poll_cycle_id": audit.get("poll_cycle_id"),
        "feed_type": feed_type,
        "outcome": audit["outcome"],
        "http_status": audit["http_status"],
        "payload_ref": audit["payload_ref"],
        "payload_sha256": audit["payload_sha256"],
        "payload_bytes": audit["payload_bytes"],
        "started_at_utc": started,
        "completed_at_utc": completed,
        "feed_timestamp": utc(audit["feed_timestamp"])
        if audit.get("feed_timestamp")
        else None,
        "feed_age_seconds": float(audit["feed_age_seconds"])
        if audit.get("feed_age_seconds") is not None
        else None,
        "incrementality": audit.get("incrementality"),
        "entity_count": audit.get("entity_count"),
        "vehicle_count": audit.get("vehicle_count"),
        "feed_version": audit.get("feed_version"),
        "audit_ref": ref,
        "audit_sha256": digest(payload),
        "raw_audit_json": canonical(audit),
        "parser_version": PARSER_VERSION,
    }
    return audit, row


def verify_payload(audit, payload, feed_type, extension):
    sha = digest(payload)
    require(audit.get("payload_sha256") == sha, "Payload SHA-256 mismatch")
    require(
        type(audit.get("payload_bytes")) is int
        and len(payload) == audit["payload_bytes"],
        "Payload size mismatch",
    )
    require(
        audit.get("payload_ref") == f"{feed_type}/{sha[:2]}/{sha}.{extension}",
        "Content-addressed path mismatch",
    )
    return sha


def verified_feed(audit, payload, feed_type, required_counts=()):
    sha = verify_payload(audit, payload, feed_type, "pb")
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(payload)
    require(
        feed.IsInitialized() and feed.header.gtfs_realtime_version,
        "Invalid GTFS-Realtime message",
    )
    require(feed.header.HasField("timestamp"), "Feed timestamp missing")
    timestamp = datetime.fromtimestamp(feed.header.timestamp, UTC)
    require(
        timestamp == utc(audit["feed_timestamp"]), "Feed timestamp disagrees with audit"
    )
    incrementality = gtfs_realtime_pb2.FeedHeader.Incrementality.Name(
        feed.header.incrementality
    )
    require(
        incrementality == audit["incrementality"], "Incrementality disagrees with audit"
    )
    age = (utc(audit["completed_at_utc"]) - timestamp).total_seconds()
    require(
        type(audit.get("feed_age_seconds")) in (int, float)
        and math.isfinite(audit["feed_age_seconds"])
        and abs(age - audit["feed_age_seconds"]) <= 0.001,
        "Freshness disagrees with audit",
    )
    require(
        type(audit.get("entity_count")) is int
        and len(feed.entity) == audit["entity_count"],
        "Entity count disagrees with audit",
    )
    for field, name in [
        ("vehicle", "vehicle_count"),
        ("trip_update", "trip_update_count"),
        ("alert", "alert_count"),
    ]:
        require(
            name in audit or name not in required_counts,
            "Entity type count missing from audit",
        )
        if name in audit:
            require(
                type(audit[name]) is int
                and sum(e.HasField(field) for e in feed.entity) == audit[name],
                "Entity type count disagrees with audit",
            )
    return sha, feed, timestamp, incrementality


def optional(message, field, convert=lambda value: value):
    """A present proto field converted, else None (also when the parent is absent)."""
    if message is None or not message.HasField(field):
        return None
    return convert(getattr(message, field))


def raw_json(message):
    return canonical(MessageToDict(message, preserving_proto_field_name=True))


def epoch_utc(value):
    return datetime.fromtimestamp(value, UTC)


def vehicle_records(audit, payload):
    sha, feed, timestamp, incrementality = verified_feed(
        audit, payload, "vehicle_positions"
    )
    rows = []
    for index, entity in enumerate(feed.entity):
        vehicle = entity.vehicle if entity.HasField("vehicle") else None
        trip = (
            vehicle.trip if vehicle is not None and vehicle.HasField("trip") else None
        )
        position = (
            vehicle.position
            if vehicle is not None and vehicle.HasField("position")
            else None
        )
        rows.append(
            {
                "payload_sha256": sha,
                "entity_index": index,
                "entity_id": entity.id,
                "feed_timestamp": timestamp,
                "incrementality": incrementality,
                "has_vehicle": vehicle is not None,
                "is_deleted": entity.is_deleted
                if entity.HasField("is_deleted")
                else None,
                "vehicle_id": vehicle.vehicle.id
                if vehicle is not None
                and vehicle.HasField("vehicle")
                and vehicle.vehicle.HasField("id")
                else None,
                "trip_id": trip.trip_id
                if trip is not None and trip.HasField("trip_id")
                else None,
                "route_id": trip.route_id
                if trip is not None and trip.HasField("route_id")
                else None,
                "vehicle_timestamp": datetime.fromtimestamp(vehicle.timestamp, UTC)
                if vehicle is not None and vehicle.HasField("timestamp")
                else None,
                "latitude": float(position.latitude)
                if position is not None and position.HasField("latitude")
                else None,
                "longitude": float(position.longitude)
                if position is not None and position.HasField("longitude")
                else None,
                "raw_entity_json": canonical(
                    MessageToDict(entity, preserving_proto_field_name=True)
                ),
                "parser_version": PARSER_VERSION,
            }
        )
    return rows


TRIP_RELATIONSHIP = gtfs_realtime_pb2.TripDescriptor.ScheduleRelationship.Name
STOP_RELATIONSHIP = (
    gtfs_realtime_pb2.TripUpdate.StopTimeUpdate.ScheduleRelationship.Name
)


def trip_update_records(audit, payload):
    """One row per stop time update; a trip without updates keeps one null-stop row."""
    sha, feed, timestamp, incrementality = verified_feed(
        audit, payload, "trip_updates", required_counts=("trip_update_count",)
    )
    rows = []
    for index, entity in enumerate(feed.entity):
        update = optional(entity, "trip_update")
        trip = optional(update, "trip")
        trip_row = {
            "payload_sha256": sha,
            "entity_index": index,
            "entity_id": entity.id,
            "feed_timestamp": timestamp,
            "incrementality": incrementality,
            "has_trip_update": update is not None,
            "trip_id": optional(trip, "trip_id"),
            "route_id": optional(trip, "route_id"),
            "direction_id": optional(trip, "direction_id"),
            "start_date": optional(trip, "start_date"),
            "trip_schedule_relationship": optional(
                trip, "schedule_relationship", TRIP_RELATIONSHIP
            ),
            "trip_timestamp": optional(update, "timestamp", epoch_utc),
            "vehicle_id": optional(optional(update, "vehicle"), "id"),
        }
        stops = list(update.stop_time_update) if update is not None else []
        # Raw trip without its stop updates; with the per-row raw stop it rebuilds
        # the entity without repeating the whole entity on every row.
        trip_only = gtfs_realtime_pb2.FeedEntity()
        trip_only.CopyFrom(entity)
        if update is not None:
            trip_only.trip_update.ClearField("stop_time_update")
        trip_row["raw_trip_json"] = raw_json(trip_only)
        # explode_outer: a trip without stop updates (e.g. CANCELED) still yields a row.
        for update_index, stop in enumerate(stops) if stops else [(None, None)]:
            arrival, departure = optional(stop, "arrival"), optional(stop, "departure")
            rows.append(
                {
                    **trip_row,
                    "update_index": update_index,
                    "stop_sequence": optional(stop, "stop_sequence"),
                    "stop_id": optional(stop, "stop_id"),
                    "stop_schedule_relationship": optional(
                        stop, "schedule_relationship", STOP_RELATIONSHIP
                    ),
                    "arrival_time": optional(arrival, "time", epoch_utc),
                    "arrival_delay": optional(arrival, "delay"),
                    "departure_time": optional(departure, "time", epoch_utc),
                    "departure_delay": optional(departure, "delay"),
                    "raw_stop_time_update_json": raw_json(stop)
                    if stop is not None
                    else None,
                    "parser_version": PARSER_VERSION,
                }
            )
    return rows


def schedule_members(payload):
    """Header and rows of each Static member present: {"routes.txt": (fields, rows)}."""
    with zipfile.ZipFile(io.BytesIO(payload)) as archive:
        names = [member.filename for member in archive.infolist()]
        require(len(names) == len(set(names)), "Invalid ZIP member names")
        members = {}
        for name in ("routes.txt", "calendar.txt", "calendar_dates.txt"):
            if name not in names:
                continue
            require(
                0 < archive.getinfo(name).file_size <= 4 * 1024 * 1024,
                f"{name} exceeds the sample limit",
            )
            reader = csv.DictReader(io.StringIO(archive.read(name).decode("utf-8-sig")))
            members[name] = (reader.fieldnames or [], list(reader))
    return members


def keyed_rows(sha, member, keys, columns, raw_column, label):
    """Bronze rows for one Static member; keys must be present and unique."""
    fieldnames, raws = member
    require(set(keys) <= set(fieldnames), f"{label} is missing key columns")
    require(len(raws) <= 50_000, f"{label} row limit exceeded")
    rows, seen = [], set()
    for raw in raws:
        key = tuple(raw.get(name) for name in keys)
        require(
            all(value and value.strip() for value in key) and key not in seen,
            f"Missing or duplicate {label} key",
        )
        seen.add(key)
        rows.append(
            {
                "static_payload_sha256": sha,
                **{name: raw.get(name) for name in columns},
                raw_column: canonical(raw),
                "parser_version": PARSER_VERSION,
            }
        )
    return rows


def route_records(audit, payload):
    sha = verify_payload(audit, payload, "gtfs_schedule", "zip")
    members = schedule_members(payload)
    require("routes.txt" in members, "routes.txt is missing")
    rows = keyed_rows(
        sha,
        members["routes.txt"],
        ["route_id"],
        ["route_id", "agency_id", "route_short_name", "route_long_name", "route_type"],
        "raw_route_json",
        "routes.txt",
    )
    require(rows, "routes.txt is empty")
    return rows


CALENDAR_DAYS = [
    "monday",
    "tuesday",
    "wednesday",
    "thursday",
    "friday",
    "saturday",
    "sunday",
]


def calendar_records(audit, payload):
    """Calendar rows of a Static version; a missing file contributes none."""
    sha = verify_payload(audit, payload, "gtfs_schedule", "zip")
    members = schedule_members(payload)
    records = {"calendar": [], "calendar_dates": []}
    if "calendar.txt" in members:
        records["calendar"] = keyed_rows(
            sha,
            members["calendar.txt"],
            ["service_id"],
            ["service_id", *CALENDAR_DAYS, "start_date", "end_date"],
            "raw_calendar_json",
            "calendar.txt",
        )
    if "calendar_dates.txt" in members:
        records["calendar_dates"] = keyed_rows(
            sha,
            members["calendar_dates.txt"],
            ["service_id", "date"],
            ["service_id", "date", "exception_type"],
            "raw_calendar_date_json",
            "calendar_dates.txt",
        )
    return records
