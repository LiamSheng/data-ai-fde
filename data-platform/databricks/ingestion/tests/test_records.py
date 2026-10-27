"""Local conversion checks; Spark/Delta behavior is accepted separately in Databricks."""

import hashlib
import io
import json
import sys
import unittest
import zipfile
from datetime import UTC, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from bronze_lib import identifier, merge_condition
from bronze_records import (
    BronzeContractError,
    audit_record,
    calendar_records,
    fingerprint,
    route_records,
    trip_update_records,
    vehicle_records,
)
from google.transit import gtfs_realtime_pb2


class BronzeRecordChecks(unittest.TestCase):
    def rt_sample(self):
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.header.gtfs_realtime_version = "2.0"
        feed.header.timestamp = 1791849030
        entity = feed.entity.add()
        entity.id = "one"
        entity.vehicle.vehicle.id = "V1"
        entity.vehicle.trip.route_id = "R1"
        payload = feed.SerializeToString()
        sha = hashlib.sha256(payload).hexdigest()
        audit = {
            "payload_sha256": sha,
            "payload_bytes": len(payload),
            "payload_ref": f"vehicle_positions/{sha[:2]}/{sha}.pb",
            "feed_timestamp": datetime.fromtimestamp(
                feed.header.timestamp, UTC
            ).isoformat(),
            "completed_at_utc": datetime.fromtimestamp(
                feed.header.timestamp + 14, UTC
            ).isoformat(),
            "feed_age_seconds": 14,
            "incrementality": "FULL_DATASET",
            "entity_count": 1,
            "vehicle_count": 1,
        }
        return audit, payload

    def static_sample(
        self, text="route_id,route_short_name,route_type\nR1,1,3\n", **members
    ):
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w") as archive:
            archive.writestr("routes.txt", text)
            for name, member in members.items():
                archive.writestr(name + ".txt", member)
        payload = buffer.getvalue()
        sha = hashlib.sha256(payload).hexdigest()
        return {
            "payload_sha256": sha,
            "payload_bytes": len(payload),
            "payload_ref": f"gtfs_schedule/{sha[:2]}/{sha}.zip",
        }, payload

    def test_missing_optional_values_stay_missing(self):
        audit, payload = self.rt_sample()
        row = vehicle_records(audit, payload)[0]
        self.assertIsNone(row["trip_id"])
        self.assertIsNone(row["vehicle_timestamp"])
        self.assertIsNone(row["is_deleted"])
        self.assertIsNone(row["latitude"])
        self.assertEqual(row["entity_index"], 0)
        self.assertEqual(row["vehicle_id"], "V1")

    def test_changed_bytes_are_rejected(self):
        audit, payload = self.rt_sample()
        with self.assertRaisesRegex(ValueError, "SHA-256"):
            vehicle_records(audit, payload + b"changed")

    def test_count_mismatch_is_rejected(self):
        audit, payload = self.rt_sample()
        audit["entity_count"] = 2
        with self.assertRaisesRegex(ValueError, "count"):
            vehicle_records(audit, payload)

    def test_content_identity_is_independent_of_attempt(self):
        audit, payload = self.rt_sample()
        one = vehicle_records(dict(audit, attempt_id="one"), payload)
        two = vehicle_records(dict(audit, attempt_id="two"), payload)
        self.assertEqual(one, two)

    def test_ingestion_time_is_excluded_from_fingerprint(self):
        one = {"key": "a", "ingested_at_utc": "first"}
        two = {"key": "a", "ingested_at_utc": "second"}
        self.assertEqual(fingerprint(one), fingerprint(two))
        self.assertNotEqual(fingerprint(one), fingerprint(dict(two, key="b")))

    def test_routes_preserve_content_version(self):
        audit, payload = self.static_sample()
        row = route_records(audit, payload)[0]
        self.assertEqual(row["static_payload_sha256"], audit["payload_sha256"])
        self.assertEqual(row["route_type"], "3")
        self.assertIsNone(row["agency_id"])

    def test_calendars_preserve_content_version(self):
        audit, payload = self.static_sample(
            calendar=(
                "service_id,monday,tuesday,wednesday,thursday,friday,saturday,"
                "sunday,start_date,end_date\n1,1,1,1,1,1,0,0,20260907,20270103\n"
            ),
            calendar_dates="service_id,date,exception_type\n1,20261012,2\n3,20261012,1\n",
        )
        records = calendar_records(audit, payload)
        (service,) = records["calendar"]
        self.assertEqual(service["static_payload_sha256"], audit["payload_sha256"])
        self.assertEqual(
            (service["service_id"], service["saturday"], service["end_date"]),
            ("1", "0", "20270103"),
        )
        self.assertEqual(
            [
                (row["service_id"], row["date"], row["exception_type"])
                for row in records["calendar_dates"]
            ],
            [("1", "20261012", "2"), ("3", "20261012", "1")],
        )

    def test_missing_calendar_files_yield_no_rows(self):
        audit, payload = self.static_sample()
        self.assertEqual(
            calendar_records(audit, payload), {"calendar": [], "calendar_dates": []}
        )

    def test_duplicate_calendar_keys_are_rejected(self):
        audit, payload = self.static_sample(
            calendar_dates="service_id,date,exception_type\n1,20261012,2\n1,20261012,1\n"
        )
        with self.assertRaisesRegex(ValueError, "duplicate"):
            calendar_records(audit, payload)

    def test_duplicate_routes_are_rejected(self):
        audit, payload = self.static_sample("route_id\nR1\nR1\n")
        with self.assertRaisesRegex(ValueError, "duplicate"):
            route_records(audit, payload)

    def test_validation_prefix_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "audit path"):
            audit_record("validation/gcs_repeat/file.json", b"{}", "vehicle_positions")

    def test_success_audit_retains_attempt_identity(self):
        attempt = "a" * 32
        audit = {
            "attempt_id": attempt,
            "feed_type": "vehicle_positions",
            "outcome": "success",
            "http_status": 200,
            "started_at_utc": "2026-10-12T23:50:00Z",
            "completed_at_utc": "2026-10-12T23:50:14Z",
            "payload_ref": "example",
            "payload_sha256": "b" * 64,
            "payload_bytes": 1,
        }
        _, record = audit_record(
            f"audit/capture_attempts/{attempt}.json",
            json.dumps(audit).encode(),
            "vehicle_positions",
        )
        self.assertEqual(record["attempt_id"], attempt)
        self.assertEqual(record["feed_type"], "vehicle_positions")

    def test_sql_identifiers_are_bounded(self):
        self.assertEqual(identifier("transit_dev_bronze"), "transit_dev_bronze")
        with self.assertRaises(ValueError):
            identifier("default; DROP TABLE other")

    def test_merge_keys_match_null_safely(self):
        # A trip without stop updates has a null update_index; plain equality
        # would never match it and re-runs would insert it again.
        self.assertEqual(
            merge_condition(["payload_sha256", "update_index"]),
            "t.payload_sha256 <=> s.payload_sha256 AND t.update_index <=> s.update_index",
        )


class TripUpdateRecordChecks(unittest.TestCase):
    def tu_sample(self, build):
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.header.gtfs_realtime_version = "2.0"
        feed.header.timestamp = 1791849030
        build(feed)
        payload = feed.SerializeToString()
        sha = hashlib.sha256(payload).hexdigest()
        audit = {
            "payload_sha256": sha,
            "payload_bytes": len(payload),
            "payload_ref": f"trip_updates/{sha[:2]}/{sha}.pb",
            "feed_timestamp": datetime.fromtimestamp(
                feed.header.timestamp, UTC
            ).isoformat(),
            "completed_at_utc": datetime.fromtimestamp(
                feed.header.timestamp + 2, UTC
            ).isoformat(),
            "feed_age_seconds": 2,
            "incrementality": "FULL_DATASET",
            "entity_count": len(feed.entity),
            "trip_update_count": sum(e.HasField("trip_update") for e in feed.entity),
        }
        return audit, payload

    @staticmethod
    def two_stop_trip(feed):
        entity = feed.entity.add()
        entity.id = "e1"
        update = entity.trip_update
        update.trip.trip_id = "T1"
        update.trip.route_id = "6641"
        update.trip.direction_id = 1
        update.trip.start_date = "20261012"
        update.trip.schedule_relationship = gtfs_realtime_pb2.TripDescriptor.SCHEDULED
        update.vehicle.id = "V9"
        update.timestamp = 1791849000
        first = update.stop_time_update.add()
        first.stop_sequence = 7
        first.stop_id = "S7"
        first.arrival.delay = -30
        first.arrival.time = 1791849100
        first.departure.delay = 0
        second = update.stop_time_update.add()
        second.stop_sequence = 8
        second.schedule_relationship = (
            gtfs_realtime_pb2.TripUpdate.StopTimeUpdate.SKIPPED
        )

    @staticmethod
    def canceled_trip(feed):
        entity = feed.entity.add()
        entity.id = "e2"
        entity.trip_update.trip.trip_id = "T2"
        entity.trip_update.trip.schedule_relationship = (
            gtfs_realtime_pb2.TripDescriptor.CANCELED
        )

    def test_trip_without_stop_updates_survives_as_one_row(self):
        audit, payload = self.tu_sample(self.canceled_trip)
        rows = trip_update_records(audit, payload)
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row["trip_id"], "T2")
        self.assertEqual(row["trip_schedule_relationship"], "CANCELED")
        self.assertTrue(row["has_trip_update"])
        for field in [
            "update_index",
            "stop_sequence",
            "stop_id",
            "stop_schedule_relationship",
            "arrival_time",
            "arrival_delay",
            "departure_time",
            "departure_delay",
        ]:
            self.assertIsNone(row[field], field)

    def test_trip_fields_repeat_on_every_stop_row(self):
        audit, payload = self.tu_sample(self.two_stop_trip)
        rows = trip_update_records(audit, payload)
        self.assertEqual([row["update_index"] for row in rows], [0, 1])
        trip_fields = {
            "payload_sha256": audit["payload_sha256"],
            "entity_index": 0,
            "entity_id": "e1",
            "feed_timestamp": datetime.fromtimestamp(1791849030, UTC),
            "incrementality": "FULL_DATASET",
            "trip_id": "T1",
            "route_id": "6641",
            "direction_id": 1,
            "start_date": "20261012",
            "trip_schedule_relationship": "SCHEDULED",
            "trip_timestamp": datetime.fromtimestamp(1791849000, UTC),
            "vehicle_id": "V9",
        }
        for row in rows:
            self.assertEqual({key: row[key] for key in trip_fields}, trip_fields)
        first, second = rows
        self.assertEqual(first["stop_sequence"], 7)
        self.assertEqual(first["stop_id"], "S7")
        self.assertEqual(first["arrival_delay"], -30)
        self.assertEqual(first["arrival_time"], datetime.fromtimestamp(1791849100, UTC))
        self.assertEqual(first["departure_delay"], 0)
        self.assertIsNone(first["departure_time"])
        self.assertEqual(second["stop_schedule_relationship"], "SKIPPED")
        self.assertIsNone(second["arrival_delay"])

    def test_raw_json_reconstructs_the_entity(self):
        audit, payload = self.tu_sample(self.two_stop_trip)
        rows = trip_update_records(audit, payload)
        trip = json.loads(rows[0]["raw_trip_json"])
        self.assertEqual(trip["id"], "e1")
        self.assertNotIn("stop_time_update", trip["trip_update"])
        self.assertEqual(rows[1]["raw_trip_json"], rows[0]["raw_trip_json"])
        stop = json.loads(rows[0]["raw_stop_time_update_json"])
        self.assertEqual(stop["stop_id"], "S7")
        self.assertEqual(stop["arrival"]["delay"], -30)

    def test_trip_without_stop_updates_has_no_raw_stop(self):
        audit, payload = self.tu_sample(self.canceled_trip)
        row = trip_update_records(audit, payload)[0]
        self.assertIsNone(row["raw_stop_time_update_json"])
        self.assertEqual(json.loads(row["raw_trip_json"])["id"], "e2")

    def test_missing_optional_trip_values_stay_missing(self):
        def bare(feed):
            entity = feed.entity.add()
            entity.id = "e3"
            entity.trip_update.trip.trip_id = "T3"
            entity.trip_update.stop_time_update.add().stop_sequence = 2

        audit, payload = self.tu_sample(bare)
        row = trip_update_records(audit, payload)[0]
        for field in [
            "route_id",
            "direction_id",
            "start_date",
            "trip_schedule_relationship",
            "trip_timestamp",
            "vehicle_id",
            "stop_id",
            "stop_schedule_relationship",
            "arrival_time",
            "arrival_delay",
        ]:
            self.assertIsNone(row[field], field)
        self.assertEqual(row["stop_sequence"], 2)

    def test_changed_bytes_are_rejected(self):
        audit, payload = self.tu_sample(self.two_stop_trip)
        with self.assertRaisesRegex(BronzeContractError, "SHA-256"):
            trip_update_records(audit, payload + b"changed")

    def test_entity_count_mismatch_is_rejected(self):
        audit, payload = self.tu_sample(self.two_stop_trip)
        audit["entity_count"] = 2
        with self.assertRaisesRegex(BronzeContractError, "count"):
            trip_update_records(audit, payload)

    def test_trip_update_count_mismatch_is_rejected(self):
        audit, payload = self.tu_sample(self.two_stop_trip)
        audit["trip_update_count"] = 2
        with self.assertRaisesRegex(BronzeContractError, "count"):
            trip_update_records(audit, payload)

    def test_trip_update_count_is_required(self):
        audit, payload = self.tu_sample(self.two_stop_trip)
        del audit["trip_update_count"]
        with self.assertRaisesRegex(BronzeContractError, "count"):
            trip_update_records(audit, payload)

    def test_vehicle_payload_path_is_rejected(self):
        audit, payload = self.tu_sample(self.two_stop_trip)
        sha = audit["payload_sha256"]
        audit["payload_ref"] = f"vehicle_positions/{sha[:2]}/{sha}.pb"
        with self.assertRaisesRegex(BronzeContractError, "path"):
            trip_update_records(audit, payload)

    def test_trip_update_audit_uses_capture_attempts_prefix(self):
        attempt = "c" * 32
        audit = {
            "attempt_id": attempt,
            "feed_type": "trip_updates",
            "outcome": "success",
            "http_status": 200,
            "started_at_utc": "2026-10-12T23:50:00Z",
            "completed_at_utc": "2026-10-12T23:50:14Z",
            "payload_ref": "example",
            "payload_sha256": "b" * 64,
            "payload_bytes": 1,
        }
        _, record = audit_record(
            f"audit/capture_attempts/{attempt}.json",
            json.dumps(audit).encode(),
            "trip_updates",
        )
        self.assertEqual(record["feed_type"], "trip_updates")


if __name__ == "__main__":
    unittest.main()
