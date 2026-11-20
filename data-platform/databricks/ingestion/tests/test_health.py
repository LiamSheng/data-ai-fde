"""Bronze health conditions (no Spark)."""

import sys
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from bronze_health import health_problems

NOW = datetime(2026, 11, 19, 20, 5, tzinfo=UTC)
HOUR = timedelta(minutes=60)
CLEAR = {"open": 0, "resolved": 2, "waived": 1}


def fresh(minutes=5):
    return {
        feed: NOW - timedelta(minutes=minutes)
        for feed in ("vehicle_positions", "trip_updates", "service_alerts")
    }


class HealthProblems(unittest.TestCase):
    def test_fresh_feeds_and_closed_quarantine_are_healthy(self):
        self.assertEqual(health_problems(fresh(), CLEAR, NOW, HOUR), [])

    def test_exactly_at_the_threshold_is_still_fresh(self):
        self.assertEqual(health_problems(fresh(60), CLEAR, NOW, HOUR), [])

    def test_a_stale_feed_is_named_with_its_age(self):
        last = fresh()
        last["trip_updates"] = NOW - timedelta(minutes=95)
        self.assertEqual(
            health_problems(last, CLEAR, NOW, HOUR),
            ["trip_updates: last successful capture 95 min ago"],
        )

    def test_a_feed_without_any_success_is_a_problem(self):
        last = fresh()
        del last["service_alerts"]
        self.assertEqual(
            health_problems(last, CLEAR, NOW, HOUR),
            ["service_alerts: no successful capture in Bronze"],
        )

    def test_an_empty_bronze_reports_every_realtime_feed(self):
        self.assertEqual(len(health_problems({}, CLEAR, NOW, HOUR)), 3)

    def test_static_schedule_age_is_not_checked(self):
        last = fresh()
        last["gtfs_schedule"] = NOW - timedelta(days=30)
        self.assertEqual(health_problems(last, CLEAR, NOW, HOUR), [])

    def test_open_quarantine_is_a_problem_but_closed_files_are_not(self):
        self.assertEqual(
            health_problems(
                fresh(), {"open": 2, "resolved": 5, "waived": 3}, NOW, HOUR
            ),
            ["2 quarantined file(s) open; see QUARANTINE.md"],
        )

    def test_problems_are_reported_together(self):
        self.assertEqual(len(health_problems({}, {"open": 1}, NOW, HOUR)), 4)

    def test_threshold_must_be_positive(self):
        for bad in (timedelta(0), timedelta(minutes=-5), 60):
            with self.subTest(max_age=bad), self.assertRaises(ValueError):
                health_problems(fresh(), CLEAR, NOW, bad)


if __name__ == "__main__":
    unittest.main()
