"""Auto Loader batch ingestion through in-memory Landing and table adapters (no Spark)."""

import hashlib
import io
import json
import sys
import unittest
import zipfile
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import bronze_autoloader
from bronze_autoloader import ingest_audits, replay_quarantine
from bronze_lib import load
from test_load import Bucket, FakeLanding, FakeTables, at

NOW = datetime(2026, 11, 5, 20, 0, tzinfo=UTC)


class AuditLanding(FakeLanding):
    """FakeLanding plus the Auto Loader adapter's read: absent objects stay absent."""

    def __init__(self):
        super().__init__()
        self.reads = []

    def read_present(self, refs):
        self.reads.append(list(refs))
        return {ref: self.objects[ref][0] for ref in refs if ref in self.objects}


class RecordingTables(FakeTables):
    """FakeTables that remembers write order and answers the replay question."""

    def __init__(self):
        super().__init__()
        self.order = []

    def merge(self, name, records):
        self.order.append(name)
        return super().merge(name, records)

    def unresolved_quarantine(self):
        events = self.rows["quarantine"].values()
        resolved = {e["audit_ref"] for e in events if e["event"] == "resolved"}
        return sorted(
            {
                e["audit_ref"]
                for e in events
                if e["event"] == "quarantined" and e["audit_ref"] not in resolved
            }
        )


class AuditBucket(Bucket):
    def __init__(self):
        super().__init__()
        self.landing = AuditLanding()

    def files(self):
        """Every audit object, as Auto Loader would discover them."""
        return sorted(
            (ref, payload)
            for ref, (payload, _) in self.landing.objects.items()
            if ref.startswith("audit/")
        )

    def alerts(self, started):
        def build(feed):
            entity = feed.entity.add(id="a1")
            entity.alert.informed_entity.add(route_id="R1")

        feed = self.feed(started - timedelta(seconds=20), build)
        return self.realtime("service_alerts", feed, started, alert_count=1)

    def failed(self, feed_type, started, outcome, error_category, payload=None):
        """A failed capture; with payload, it failed after the payload was saved."""
        attempt = self.attempt_id()
        audit = {
            "attempt_id": attempt,
            "feed_type": feed_type,
            "outcome": outcome,
            "error_category": error_category,
            "source_id": "primary",
            "started_at_utc": started.isoformat(),
            "completed_at_utc": (started + timedelta(seconds=1)).isoformat(),
        }
        if payload is not None:
            sha = hashlib.sha256(payload).hexdigest()
            audit.update(
                {
                    "http_status": 200,
                    "payload_ref": f"{feed_type}/{sha[:2]}/{sha}.pb",
                    "payload_sha256": sha,
                    "payload_bytes": len(payload),
                }
            )
            self.landing.put(audit["payload_ref"], payload, started)
        ref = f"audit/capture_attempts/{attempt}.json"
        self.landing.put(ref, json.dumps(audit).encode(), started)
        return ref

    def rewrite_audit(self, ref, **fields):
        audit = json.loads(self.landing.objects[ref][0])
        audit.update(fields)
        self.landing.put(ref, json.dumps(audit).encode(), self.landing.objects[ref][1])

    def payload_ref(self, ref):
        return json.loads(self.landing.objects[ref][0])["payload_ref"]


def raw_snapshot(payload, feed_type="trip_updates"):
    """Fields of a successful audit for arbitrary payload bytes."""
    sha = hashlib.sha256(payload).hexdigest()
    return {
        "payload_ref": f"{feed_type}/{sha[:2]}/{sha}.pb",
        "payload_sha256": sha,
        "payload_bytes": len(payload),
    }


class BatchCase(unittest.TestCase):
    def setUp(self):
        self.bucket = AuditBucket()
        self.tables = RecordingTables()

    def ingest(self):
        return ingest_audits(self.bucket.landing, self.tables, self.bucket.files(), NOW)

    def quarantine(self):
        return {
            row["audit_ref"]: row
            for row in self.tables.rows["quarantine"].values()
            if row["event"] == "quarantined"
        }


class AutoLoaderBatch(BatchCase):
    def test_each_feed_lands_in_its_tables(self):
        started = at("2026-10-13T18:00:00")
        self.bucket.vehicles(started)
        self.bucket.trip_updates(started)
        self.bucket.alerts(started)
        self.bucket.schedule(
            started,
            calendar_dates="service_id,date,exception_type\n3,20261013,1\n",
        )
        result = self.ingest()
        self.assertEqual(result["quarantined"], 0)
        for name, rows in (
            ("vehicle_positions", 1),
            ("trip_updates", 2),
            ("service_alerts", 1),
            ("routes", 1),
            ("calendar_dates", 1),
            ("capture_attempts", 4),
        ):
            self.assertEqual(self.tables.count(name), rows, name)

    def test_rediscovered_files_insert_nothing(self):
        started = at("2026-10-13T18:00:00")
        self.bucket.trip_updates(started)
        self.bucket.alerts(started)
        self.ingest()
        second = self.ingest()
        for name, written in second["tables"].items():
            self.assertEqual(written["inserted_rows"], 0, name)

    def test_writes_content_then_quarantine_then_attempts(self):
        good = self.bucket.trip_updates(at("2026-10-13T18:00:00"), trip_id="A")
        self.bucket.trip_updates(at("2026-10-13T18:10:00"), trip_id="B", entity_count=7)
        self.ingest()
        self.assertEqual(
            self.tables.order, ["trip_updates", "quarantine", "capture_attempts"]
        )
        self.assertEqual(
            [r["audit_ref"] for r in self.tables.rows["capture_attempts"].values()],
            [good],
        )

    def test_identical_bytes_captured_twice_convert_to_one_snapshot(self):
        feed_time = at("2026-10-13T17:59:30")
        for started in ("2026-10-13T18:00:00", "2026-10-13T18:00:40"):
            self.bucket.trip_updates(at(started), feed_time=feed_time)
        result = self.ingest()
        self.assertEqual(result["tables"]["trip_updates"]["inserted_rows"], 2)
        self.assertEqual(self.tables.count("capture_attempts"), 2)


class FailedCaptures(BatchCase):
    def test_failure_before_payload_is_recorded_without_reading(self):
        ref = self.bucket.failed(
            "trip_updates", at("2026-10-13T18:00:00"), "timeout", "timeout"
        )
        result = self.ingest()
        (row,) = self.tables.rows["capture_attempts"].values()
        self.assertEqual(
            (row["audit_ref"], row["outcome"], row["payload_ref"], row["http_status"]),
            (ref, "timeout", None, None),
        )
        self.assertEqual(result["failed_captures"], 1)
        self.assertEqual(self.bucket.landing.reads, [[]])
        self.assertEqual(self.tables.count("quarantine"), 0)

    def test_failure_after_payload_is_recorded_but_not_converted(self):
        self.bucket.failed(
            "trip_updates",
            at("2026-10-13T18:00:00"),
            "invalid_protobuf",
            "invalid_protobuf",
            payload=b"\xff\xff\xff",
        )
        self.ingest()
        (row,) = self.tables.rows["capture_attempts"].values()
        self.assertEqual(row["outcome"], "invalid_protobuf")
        self.assertIsNotNone(row["payload_ref"])
        self.assertEqual(self.tables.count("trip_updates"), 0)
        self.assertEqual(self.tables.count("quarantine"), 0)

    def test_failure_must_name_its_error_category(self):
        ref = self.bucket.failed(
            "trip_updates", at("2026-10-13T18:00:00"), "timeout", "timeout"
        )
        self.bucket.rewrite_audit(ref, error_category=None)
        self.ingest()
        self.assertEqual(
            self.quarantine()[ref]["failed_check"], "Failed audit has no error category"
        )


class ContractFailures(BatchCase):
    """A file that breaks the source contract is set aside; the batch goes on."""

    def assert_quarantined(self, ref, check):
        good = self.bucket.trip_updates(at("2026-10-13T19:00:00"), trip_id="GOOD")
        result = self.ingest()
        self.assertEqual(self.quarantine()[ref]["failed_check"], check)
        attempts = {
            r["audit_ref"] for r in self.tables.rows["capture_attempts"].values()
        }
        self.assertNotIn(ref, attempts)
        self.assertIn(good, attempts)
        self.assertEqual(result["quarantined"], 1)
        return self.quarantine()[ref]

    def test_payload_size_mismatch(self):
        ref = self.bucket.trip_updates(at("2026-10-13T18:00:00"))
        self.bucket.rewrite_audit(ref, payload_bytes=1)
        self.assert_quarantined(ref, "Payload size mismatch")

    def test_count_disagreement(self):
        ref = self.bucket.trip_updates(at("2026-10-13T18:00:00"), entity_count=7)
        self.assert_quarantined(ref, "Entity count disagrees with audit")

    def test_missing_payload(self):
        ref = self.bucket.trip_updates(at("2026-10-13T18:00:00"))
        del self.bucket.landing.objects[self.bucket.payload_ref(ref)]
        self.assert_quarantined(ref, "Payload missing for successful audit")

    def test_payload_that_is_not_protobuf(self):
        ref = self.bucket.trip_updates(at("2026-10-13T18:00:00"))
        garbage = b"\xff\xff\xff"
        self.bucket.landing.put(raw_snapshot(garbage)["payload_ref"], garbage, NOW)
        self.bucket.rewrite_audit(ref, **raw_snapshot(garbage))
        self.assert_quarantined(ref, "Invalid GTFS-Realtime message")

    def test_static_payload_that_is_not_a_zip(self):
        ref = self.bucket.schedule(at("2026-10-13T12:00:00"))
        garbage = b"not a zip"
        sha = hashlib.sha256(garbage).hexdigest()
        payload_ref = f"gtfs_schedule/{sha[:2]}/{sha}.zip"
        self.bucket.landing.put(payload_ref, garbage, NOW)
        self.bucket.rewrite_audit(
            ref, payload_ref=payload_ref, payload_sha256=sha, payload_bytes=len(garbage)
        )
        self.assert_quarantined(ref, "Unreadable Static ZIP")

    def test_static_row_longer_than_its_header(self):
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w") as archive:
            archive.writestr("routes.txt", "route_id\nR1,extra\n")
        ref = self.bucket.audit(
            "gtfs_schedule", buffer.getvalue(), "zip", at("2026-10-13T12:00:00")
        )
        self.assert_quarantined(ref, "routes.txt has malformed rows")

    def test_unreadable_audit_has_no_feed_type(self):
        ref = f"audit/capture_attempts/{'e' * 32}.json"
        self.bucket.landing.put(ref, b"{not json", NOW)
        row = self.assert_quarantined(ref, "Unreadable audit")
        self.assertIsNone(row["feed_type"])

    def test_nan_in_an_audit_is_unreadable(self):
        ref = self.bucket.trip_updates(at("2026-10-13T18:00:00"))
        text = self.bucket.landing.objects[ref][0].decode()
        self.bucket.landing.put(
            ref, text.replace('"http_status": 200', '"http_status": NaN').encode(), NOW
        )
        self.assert_quarantined(ref, "Unreadable audit")

    def test_feed_type_must_match_the_audit_prefix(self):
        ref = self.bucket.trip_updates(at("2026-10-13T18:00:00"))
        self.bucket.rewrite_audit(ref, feed_type="gtfs_schedule")
        self.assert_quarantined(ref, "Unexpected feed type for audit path")

    def test_mistyped_audit_field(self):
        ref = self.bucket.trip_updates(at("2026-10-13T18:00:00"))
        self.bucket.rewrite_audit(ref, vehicle_count="0")
        self.assert_quarantined(ref, "Invalid audit field")

    def test_missing_audit_timestamp(self):
        ref = self.bucket.failed(
            "trip_updates", at("2026-10-13T18:00:00"), "timeout", "timeout"
        )
        self.bucket.rewrite_audit(ref, started_at_utc=None)
        self.assert_quarantined(ref, "UTC timestamp required")

    def test_payload_path_outside_its_feed_prefix_is_never_read(self):
        ref = self.bucket.trip_updates(at("2026-10-13T18:00:00"))
        self.bucket.rewrite_audit(ref, payload_ref="../secrets/key.pb")
        self.assert_quarantined(ref, "Content-addressed path mismatch")
        self.assertNotIn(
            "../secrets/key.pb", [r for refs in self.bucket.landing.reads for r in refs]
        )

    def test_same_bad_file_found_again_adds_no_event(self):
        ref = self.bucket.trip_updates(at("2026-10-13T18:00:00"), entity_count=7)
        self.ingest()
        second = self.ingest()
        self.assertEqual(second["tables"]["quarantine"]["inserted_rows"], 0)
        self.assertEqual(list(self.quarantine()), [ref])


class SystemFailures(BatchCase):
    """Anything that is not one file's contract failure stops the batch."""

    def test_unexpected_conversion_error_writes_nothing(self):
        self.bucket.trip_updates(at("2026-10-13T18:00:00"))

        def broken(audit, payload):
            raise RuntimeError("parser bug")

        feeds = dict(bronze_autoloader.FEEDS, trip_updates=("pb", 1 << 24, broken))
        with (
            mock.patch.object(bronze_autoloader, "FEEDS", feeds),
            self.assertRaisesRegex(RuntimeError, "parser bug"),
        ):
            self.ingest()
        self.assertEqual(self.tables.writes, 0)

    def test_table_error_leaves_no_attempt_rows(self):
        self.bucket.trip_updates(at("2026-10-13T18:00:00"))
        failing = self.tables.merge

        def merge(name, records):
            if name == "trip_updates":
                raise RuntimeError("Existing table schema does not match contract")
            return failing(name, records)

        self.tables.merge = merge
        with self.assertRaisesRegex(RuntimeError, "schema"):
            self.ingest()
        self.assertEqual(self.tables.count("capture_attempts"), 0)


class QuarantineReplay(BatchCase):
    def setUp(self):
        super().setUp()
        self.ref = self.bucket.trip_updates(at("2026-10-13T18:00:00"))
        self.payload_ref = self.bucket.payload_ref(self.ref)
        self.payload = self.bucket.landing.objects.pop(self.payload_ref)
        self.ingest()

    def test_file_that_now_converts_is_resolved_after_its_attempt(self):
        self.bucket.landing.objects[self.payload_ref] = self.payload
        self.tables.order.clear()
        result = replay_quarantine(self.bucket.landing, self.tables, NOW)
        self.assertEqual(result["resolved"], 1)
        self.assertEqual(
            self.tables.order, ["trip_updates", "capture_attempts", "quarantine"]
        )
        self.assertEqual(self.tables.unresolved_quarantine(), [])
        self.assertEqual(
            replay_quarantine(self.bucket.landing, self.tables, NOW)[
                "unresolved_before"
            ],
            0,
        )

    def test_file_still_failing_stays_unresolved_without_new_events(self):
        events = self.tables.count("quarantine")
        result = replay_quarantine(self.bucket.landing, self.tables, NOW)
        self.assertEqual(result["resolved"], 0)
        self.assertEqual(self.tables.count("quarantine"), events)
        self.assertEqual(self.tables.unresolved_quarantine(), [self.ref])


class CutoverFromBoundedLoader(unittest.TestCase):
    """Rows the bounded loader wrote are the rows Auto Loader would write."""

    def test_rediscovering_bounded_loads_inserts_nothing(self):
        bucket, tables = AuditBucket(), RecordingTables()
        rt_ref = bucket.vehicles(at("2026-10-13T18:00:00"))
        static_ref = bucket.schedule(at("2026-10-13T12:00:00"))
        bucket.trip_updates(at("2026-10-13T18:00:00"), trip_id="A")
        bucket.trip_updates(at("2026-10-13T18:10:00"), trip_id="B")
        load(
            bucket.landing,
            tables,
            rt_ref,
            static_ref,
            date(2026, 10, 13),
            date(2026, 10, 13),
            now=at("2026-10-14T00:00:00"),
        )
        result = ingest_audits(bucket.landing, tables, bucket.files(), NOW)
        for name, written in result["tables"].items():
            self.assertEqual(written["inserted_rows"], 0, name)


class AlertCount(unittest.TestCase):
    def test_service_alert_audits_need_their_alert_count(self):
        bucket, tables = AuditBucket(), RecordingTables()
        feed = Bucket.feed(
            at("2026-10-13T17:59:40"),
            lambda f: f.entity.add(id="a").alert.SetInParent(),
        )
        ref = bucket.realtime("service_alerts", feed, at("2026-10-13T18:00:00"))
        ingest_audits(bucket.landing, tables, bucket.files(), NOW)
        (event,) = tables.rows["quarantine"].values()
        self.assertEqual(
            (event["audit_ref"], event["failed_check"]),
            (ref, "Entity type count missing from audit"),
        )


if __name__ == "__main__":
    unittest.main()
