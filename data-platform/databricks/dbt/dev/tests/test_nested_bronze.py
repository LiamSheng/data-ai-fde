"""Exercise the offline warehouse's nested Bronze types without compiling dbt."""

import importlib.util
import unittest
from datetime import UTC, datetime
from pathlib import Path

path = Path(__file__).resolve().parents[1] / "duckdb_check.py"
spec = importlib.util.spec_from_file_location("nested_warehouse_checker", path)
checker = importlib.util.module_from_spec(spec)
spec.loader.exec_module(checker)


class NestedBronzeWarehouseChecks(unittest.TestCase):
    def setUp(self):
        self.name = "workspace.transit_dev_bronze.service_alerts"
        self.warehouse = checker.Warehouse(
            {
                "nodes": {},
                "sources": {
                    "source.test.service_alerts": {
                        "name": "service_alerts",
                        "relation_name": self.name,
                    }
                },
            }
        )
        self.addCleanup(self.warehouse.con.close)

    def test_nested_arrays_and_timestamps_round_trip(self):
        when = datetime(2026, 10, 12, 16, 30, tzinfo=UTC)
        row = {
            "entity_index": 0,
            "feed_timestamp": when,
            "informed_entity": [
                {
                    "agency_id": None,
                    "route_id": "R1",
                    "route_type": None,
                    "direction_id": 0,
                    "stop_id": None,
                    "trip": {
                        "trip_id": "T1",
                        "route_id": None,
                        "direction_id": None,
                        "start_date": None,
                        "start_time": None,
                        "schedule_relationship": None,
                    },
                }
            ],
            "active_period": [{"start_utc": when, "end_utc": None}],
        }
        self.warehouse.insert(self.name, [row])
        actual = self.warehouse.query(
            f"SELECT entity_index, feed_timestamp, informed_entity, active_period FROM {self.name}"
        )
        self.assertEqual(actual, [row])
        expected = {
            **row,
            "feed_timestamp": "2026-10-12 16:30:00",
            "active_period": [{"start_utc": "2026-10-12 16:30:00", "end_utc": None}],
        }
        self.assertEqual(
            checker.projected(actual, row), checker.projected([expected], row)
        )

    def test_fixture_replacement_preserves_null_and_empty_arrays(self):
        rows = [
            {"entity_index": 0, "informed_entity": None, "active_period": None},
            {"entity_index": 1, "informed_entity": [], "active_period": []},
        ]
        self.warehouse.replace_with_rows(self.name, rows)
        actual = self.warehouse.query(
            f"SELECT entity_index, informed_entity, active_period FROM {self.name} ORDER BY entity_index"
        )
        self.assertEqual(actual, rows)
