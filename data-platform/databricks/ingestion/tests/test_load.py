"""Batch ingestion checks through in-memory Landing and table adapters (no Spark)."""

import hashlib
import io
import json
import re
import sys
import unittest
import zipfile
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from bronze_lib import KEYS, load, trip_update_range
from bronze_records import BronzeContractError, require
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

    def existing_attempt_ids(self):
        return {key[0] for key in self.rows["capture_attempts"]}

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


class DateRangeIngestion(unittest.TestCase):
    def setUp(self):
        self.bucket = Bucket()
        self.tables = FakeTables()
        self.rt_ref = self.bucket.vehicles(at("2026-10-13T18:00:00"))
        self.static_ref = self.bucket.schedule(at("2026-10-13T12:00:00"))

    def load(self, start, end):
        return load(
            self.bucket.landing,
            self.tables,
            self.rt_ref,
            self.static_ref,
            date.fromisoformat(start),
            date.fromisoformat(end),
        )

    def test_repeating_a_range_adds_no_rows(self):
        self.bucket.trip_updates(at("2026-10-13T18:00:00"), trip_id="A")
        self.bucket.trip_updates(at("2026-10-14T18:00:00"), trip_id="B")
        first = self.load("2026-10-13", "2026-10-14")
        self.assertEqual(first["tables"]["trip_updates"]["inserted_rows"], 4)
        self.assertEqual(self.tables.count("capture_attempts"), 4)

        second = self.load("2026-10-13", "2026-10-14")
        for name, result in second["tables"].items():
            self.assertEqual(result["inserted_rows"], 0, name)
        self.assertEqual(self.tables.count("trip_updates"), 4)
        self.assertEqual(self.tables.count("capture_attempts"), 4)

    def test_overlapping_range_only_adds_missing_snapshots(self):
        self.bucket.trip_updates(at("2026-10-13T18:00:00"), trip_id="A")
        self.bucket.trip_updates(at("2026-10-14T18:00:00"), trip_id="B")
        self.bucket.trip_updates(at("2026-10-15T18:00:00"), trip_id="C")
        self.load("2026-10-13", "2026-10-14")

        second = self.load("2026-10-14", "2026-10-15")
        self.assertEqual(second["trip_update_range"]["audits"], 2)
        self.assertEqual(second["trip_update_range"]["new_audits"], 1)
        self.assertEqual(second["tables"]["trip_updates"]["inserted_rows"], 2)
        self.assertEqual(second["tables"]["capture_attempts"]["inserted_rows"], 1)
        self.assertEqual(self.tables.count("trip_updates"), 6)

    def test_range_uses_vancouver_dates_of_the_attempt_start(self):
        # 06:59 UTC on Oct 14 is 23:59 on Oct 13 in Vancouver; 07:00 is Oct 14.
        self.bucket.trip_updates(at("2026-10-14T06:59:00"), trip_id="LATE_OCT_13")
        self.bucket.trip_updates(at("2026-10-14T07:00:00"), trip_id="EARLY_OCT_14")
        result = self.load("2026-10-13", "2026-10-13")
        self.assertEqual(result["trip_update_range"]["audits"], 1)
        self.assertEqual(
            {row["trip_id"] for row in self.tables.rows["trip_updates"].values()},
            {"LATE_OCT_13"},
        )

    def test_failed_and_other_feed_audits_are_not_selected(self):
        self.bucket.trip_updates(at("2026-10-13T18:00:00"), trip_id="A")
        self.bucket.trip_updates(
            at("2026-10-13T18:10:00"), trip_id="FAILED", outcome="http_error"
        )
        result = self.load("2026-10-13", "2026-10-13")
        self.assertEqual(result["trip_update_range"]["audits"], 1)
        # The VP audit in the same prefix and window is loaded as the fixed input only.
        self.assertEqual(self.tables.count("capture_attempts"), 3)

    def test_one_invalid_audit_fails_the_batch_before_any_write(self):
        self.bucket.trip_updates(at("2026-10-13T18:00:00"), trip_id="A")
        # The audit disagrees with its payload's entity count.
        self.bucket.trip_updates(at("2026-10-13T18:10:00"), trip_id="B", entity_count=7)
        with self.assertRaisesRegex(BronzeContractError, "Entity count"):
            self.load("2026-10-13", "2026-10-13")
        self.assertEqual(self.tables.writes, 0)

    def test_invalid_audit_record_fails_the_batch_before_any_write(self):
        self.bucket.trip_updates(at("2026-10-13T18:00:00"), trip_id="A")
        self.bucket.trip_updates(
            at("2026-10-13T18:10:00"), trip_id="B", http_status=500
        )
        with self.assertRaisesRegex(BronzeContractError, "HTTP success"):
            self.load("2026-10-13", "2026-10-13")
        self.assertEqual(self.tables.writes, 0)

    def test_identical_payloads_load_as_one_snapshot(self):
        feed_time = at("2026-10-13T17:59:30")
        for started in ("2026-10-13T18:00:00", "2026-10-13T18:00:40"):
            self.bucket.trip_updates(at(started), feed_time=feed_time)
        result = self.load("2026-10-13", "2026-10-13")
        self.assertEqual(result["trip_update_range"]["audits"], 2)
        self.assertEqual(result["trip_update_range"]["snapshots"], 1)
        self.assertEqual(result["tables"]["trip_updates"]["inserted_rows"], 2)
        self.assertEqual(self.tables.count("capture_attempts"), 4)

    def test_unreadable_audit_in_the_window_fails_the_batch(self):
        self.bucket.trip_updates(at("2026-10-13T18:00:00"), trip_id="A")
        self.bucket.landing.put(
            f"audit/capture_attempts/{'f' * 32}.json",
            b"{not json",
            at("2026-10-13T18:05:00"),
        )
        with self.assertRaisesRegex(BronzeContractError, "Unreadable audit"):
            self.load("2026-10-13", "2026-10-13")
        self.assertEqual(self.tables.writes, 0)

    def test_empty_range_is_a_contract_error(self):
        with self.assertRaisesRegex(BronzeContractError, "No successful Trip Updates"):
            self.load("2026-10-13", "2026-10-13")
        self.assertEqual(self.tables.writes, 0)


class LatestStaticIngestion(unittest.TestCase):
    def setUp(self):
        self.bucket = Bucket()
        self.tables = FakeTables()
        self.rt_ref = self.bucket.vehicles(at("2026-10-13T18:00:00"))
        self.bucket.trip_updates(at("2026-10-13T18:00:00"))
        self.static_ref = self.bucket.schedule(
            at("2026-10-01T16:00:00"), short_name="REFERENCE"
        )

    def load(self):
        return load(
            self.bucket.landing,
            self.tables,
            self.rt_ref,
            self.static_ref,
            date(2026, 10, 13),
            date(2026, 10, 13),
        )

    def short_names(self):
        return {row["route_short_name"] for row in self.tables.rows["routes"].values()}

    def test_latest_successful_capture_is_loaded_beside_the_reference(self):
        # Written first, so its audit path sorts before the older capture's.
        self.bucket.schedule(at("2026-10-12T16:00:00"), short_name="LATEST")
        self.bucket.schedule(at("2026-10-11T16:00:00"), short_name="OLD")
        # Later, but failed: never selected.
        self.bucket.schedule(
            at("2026-10-13T16:00:00"), short_name="FAILED", outcome="http_error"
        )
        self.load()
        self.assertEqual(self.short_names(), {"REFERENCE", "LATEST"})

    def test_reference_that_is_the_latest_loads_once(self):
        self.load()
        self.assertEqual(self.short_names(), {"REFERENCE"})
        self.assertEqual(self.tables.count("capture_attempts"), 3)

    def test_completion_tie_takes_the_smallest_payload_hash_like_dim_routes(self):
        refs = [
            self.bucket.schedule(at("2026-10-12T16:00:00"), short_name=name)
            for name in ("A", "B", "C")
        ]
        hashes = {
            json.loads(self.bucket.landing.objects[ref][0])["payload_sha256"]
            for ref in refs
        }
        self.load()
        loaded = {
            row["static_payload_sha256"]
            for row in self.tables.rows["routes"].values()
            if row["route_short_name"] != "REFERENCE"
        }
        self.assertEqual(loaded, {min(hashes)})

    def test_calendar_of_each_loaded_version_is_written(self):
        self.bucket.schedule(
            at("2026-10-12T16:00:00"),
            short_name="LATEST",
            calendar_dates="service_id,date,exception_type\n3,20261012,1\n",
        )
        result = self.load()
        self.assertEqual(result["tables"]["calendar_dates"]["inserted_rows"], 1)
        # The reference ZIP has no calendar files, so it adds no calendar rows.
        self.assertEqual(self.tables.count("calendar"), 0)
        self.assertEqual(self.load()["tables"]["calendar_dates"]["inserted_rows"], 0)

    def test_version_already_in_bronze_adds_no_routes(self):
        self.bucket.schedule(at("2026-10-12T16:00:00"))
        self.load()
        # The next daily capture downloads the same published version.
        self.bucket.schedule(at("2026-10-13T16:00:00"))
        result = self.load()
        self.assertEqual(result["tables"]["routes"]["inserted_rows"], 0)
        self.assertEqual(self.short_names(), {"REFERENCE", "1"})
        # Its audit is still recorded, so the version's latest capture moves forward.
        self.assertEqual(result["tables"]["capture_attempts"]["inserted_rows"], 1)


class TripUpdateRangeParameters(unittest.TestCase):
    def test_default_is_the_last_fourteen_vancouver_days(self):
        # 03:00 UTC on Oct 17 is still Oct 16 in Vancouver.
        now = at("2026-10-17T03:00:00")
        self.assertEqual(
            trip_update_range("", "14", now),
            (date(2026, 10, 3), date(2026, 10, 16)),
        )

    def test_explicit_end_date_and_days(self):
        self.assertEqual(
            trip_update_range("2026-10-14", "2", at("2026-10-17T03:00:00")),
            (date(2026, 10, 13), date(2026, 10, 14)),
        )

    def test_days_must_be_a_positive_whole_number(self):
        for days in ("0", "-1", "two", "1.5"):
            with self.assertRaisesRegex(BronzeContractError, "tu_days"):
                trip_update_range("", days, at("2026-10-17T03:00:00"))

    def test_end_date_must_be_iso(self):
        with self.assertRaisesRegex(BronzeContractError, "tu_end_date"):
            trip_update_range("10/14/2026", "1", at("2026-10-17T03:00:00"))


if __name__ == "__main__":
    unittest.main()
