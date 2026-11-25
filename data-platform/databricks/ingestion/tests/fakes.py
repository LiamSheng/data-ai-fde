"""In-memory Landing and Bronze table adapters for ingestion tests (no Spark)."""

import hashlib
import io
import json
import re
import sys
import zipfile
from datetime import UTC, datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from bronze_lib import KEYS
from bronze_records import require
from google.transit import gtfs_realtime_pb2


class FakeLanding:
    """Landing bucket objects with their modification times."""

    def __init__(self):
        self.objects = {}

    def put(self, ref, payload, modified):
        self.objects[ref] = (payload, modified)

    def list(self, prefix, modified_after, modified_before):
        return sorted(
            ref
            for ref, (_, modified) in self.objects.items()
            if ref.startswith(prefix) and modified_after <= modified < modified_before
        )

    def read_many(self, refs, limit, pattern):
        # Same checks and messages as SparkLanding.read_many.
        for ref in refs:
            require(
                re.fullmatch(pattern, ref),
                "Object path outside the selected production prefix",
            )
        require(
            all(ref in self.objects for ref in refs),
            "Expected one exact object per path",
        )
        found = {ref: self.objects[ref][0] for ref in refs}
        for payload in found.values():
            require(0 < len(payload) <= limit, "Missing or oversized sample file")
        return found


class FakeTables:
    """Insert-only tables keyed like Bronze; existing keys must hold identical content."""

    def __init__(self):
        self.rows = {name: {} for name in KEYS}
        self.writes = 0

    def merge(self, name, records):
        self.writes += 1
        table, keys = self.rows[name], KEYS[name]
        batch = {tuple(row[key] for key in keys): row for row in records}
        require(len(batch) == len(records), "Input contains duplicate immutable keys")
        existing = 0
        for key, row in batch.items():
            if key in table:
                require(
                    table[key]["record_sha256"] == row["record_sha256"],
                    "Immutable key collision: existing content differs",
                )
                existing += 1
        for key, row in batch.items():
            table.setdefault(key, row)
        return {
            "input_rows": len(records),
            "existing_rows": existing,
            "inserted_rows": len(records) - existing,
        }

    def count(self, name):
        return len(self.rows[name])


def at(text):
    return datetime.fromisoformat(text).replace(tzinfo=UTC)


class Bucket:
    """Builds audit and payload objects the way the collector writes them."""

    def __init__(self):
        self.landing = FakeLanding()
        self.count = 0

    def attempt_id(self):
        self.count += 1
        return f"{self.count:032x}"

    def audit(self, feed_type, payload, extension, started, **fields):
        attempt = self.attempt_id()
        sha = hashlib.sha256(payload).hexdigest()
        payload_ref = f"{feed_type}/{sha[:2]}/{sha}.{extension}"
        completed = started + timedelta(seconds=2)
        audit = {
            "attempt_id": attempt,
            "feed_type": feed_type,
            "outcome": "success",
            "http_status": 200,
            "started_at_utc": started.isoformat(),
            "completed_at_utc": completed.isoformat(),
            "payload_ref": payload_ref,
            "payload_sha256": sha,
            "payload_bytes": len(payload),
            **fields,
        }
        prefix = (
            "schedule_capture_attempts"
            if feed_type == "gtfs_schedule"
            else "capture_attempts"
        )
        ref = f"audit/{prefix}/{attempt}.json"
        self.landing.put(payload_ref, payload, completed)
        self.landing.put(ref, json.dumps(audit).encode(), completed)
        return ref

    @staticmethod
    def feed(timestamp, build):
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.header.gtfs_realtime_version = "2.0"
        feed.header.timestamp = int(timestamp.timestamp())
        build(feed)
        return feed

    def realtime(self, feed_type, feed, started, **outcome):
        payload = feed.SerializeToString()
        feed_time = datetime.fromtimestamp(feed.header.timestamp, UTC)
        completed = started + timedelta(seconds=2)
        return self.audit(
            feed_type,
            payload,
            "pb",
            started,
            **{
                "feed_timestamp": feed_time.isoformat(),
                "feed_age_seconds": (completed - feed_time).total_seconds(),
                "incrementality": "FULL_DATASET",
                "entity_count": len(feed.entity),
                "vehicle_count": sum(e.HasField("vehicle") for e in feed.entity),
                "trip_update_count": sum(
                    e.HasField("trip_update") for e in feed.entity
                ),
                **outcome,
            },
        )

    def vehicles(self, started):
        def build(feed):
            entity = feed.entity.add()
            entity.id = "v1"
            entity.vehicle.vehicle.id = "V1"

        return self.realtime("vehicle_positions", self.feed(started, build), started)

    def schedule(self, started, short_name="1", calendar_dates=None, **outcome):
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w") as archive:
            archive.writestr(
                "routes.txt", f"route_id,route_short_name\nR1,{short_name}\n"
            )
            if calendar_dates:
                archive.writestr("calendar_dates.txt", calendar_dates)
        return self.audit("gtfs_schedule", buffer.getvalue(), "zip", started, **outcome)

    def trip_updates(self, started, trip_id="T1", feed_time=None, **outcome):
        """One Trip Updates capture: a two-stop trip observed at feed_time."""

        def build(feed):
            entity = feed.entity.add()
            entity.id = trip_id
            update = entity.trip_update
            update.trip.trip_id = trip_id
            update.trip.start_date = "20261013"
            for sequence in (4, 5):
                stop = update.stop_time_update.add()
                stop.stop_sequence = sequence
                stop.arrival.delay = 60

        feed = self.feed(feed_time or started - timedelta(seconds=20), build)
        return self.realtime("trip_updates", feed, started, **outcome)
