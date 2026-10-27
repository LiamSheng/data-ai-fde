"""Tests for the JSON-ready capture attempt record."""

import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from transit_realtime_pipeline.models import (
    CaptureAttempt,
    ErrorCategory,
    Outcome,
)

STARTED = datetime(2026, 9, 22, 17, 0, tzinfo=UTC)
DIGEST = "a" * 64
PAYLOAD_REF = f"vehicle_positions/aa/{DIGEST}.pb"


def successful_attempt() -> CaptureAttempt:
    """Build a synthetic attempt; no network or file access occurs."""
    return CaptureAttempt(
        attempt_id="attempt-001",
        source_id="primary",
        feed_type="vehicle_positions",
        started_at_utc=STARTED,
        completed_at_utc=STARTED + timedelta(seconds=1),
        outcome=Outcome.SUCCESS,
        http_status=200,
        payload_ref=PAYLOAD_REF,
        payload_sha256=DIGEST,
        payload_bytes=128,
        feed_timestamp=STARTED,
        feed_age_seconds=1.0,
        incrementality="FULL_DATASET",
        entity_count=0,
    )


def test_success_record_has_only_approved_json_fields() -> None:
    record = successful_attempt().to_record()

    # Exact equality also catches any unexpected field added to the output.
    assert record == {
        "attempt_id": "attempt-001",
        "source_id": "primary",
        "feed_type": "vehicle_positions",
        "started_at_utc": "2026-09-22T17:00:00Z",
        "completed_at_utc": "2026-09-22T17:00:01Z",
        "outcome": "success",
        "http_status": 200,
        "payload_ref": PAYLOAD_REF,
        "payload_sha256": DIGEST,
        "payload_bytes": 128,
        "feed_timestamp": "2026-09-22T17:00:00Z",
        "feed_age_seconds": 1.0,
        "incrementality": "FULL_DATASET",
        "entity_count": 0,
    }

    # The returned values can be encoded as strict JSON.
    assert json.loads(json.dumps(record, allow_nan=False)) == record


def test_401_record_omits_unknown_payload_and_feed_fields() -> None:
    attempt = CaptureAttempt(
        attempt_id="attempt-002",
        source_id="primary",
        feed_type="vehicle_positions",
        started_at_utc=STARTED,
        completed_at_utc=STARTED + timedelta(seconds=1),
        outcome=Outcome.HTTP_ERROR,
        http_status=401,
        error_category=ErrorCategory.AUTHENTICATION,
    )

    assert attempt.to_record() == {
        "attempt_id": "attempt-002",
        "source_id": "primary",
        "feed_type": "vehicle_positions",
        "started_at_utc": "2026-09-22T17:00:00Z",
        "completed_at_utc": "2026-09-22T17:00:01Z",
        "outcome": "http_error",
        "http_status": 401,
        "error_category": "authentication",
    }


@pytest.mark.parametrize(
    "invalid_age",
    [float("nan"), float("inf"), float("-inf")],
)
def test_non_finite_feed_age_cannot_enter_a_record(
    invalid_age: float,
) -> None:
    with pytest.raises(ValueError, match="feed_age_seconds must be finite"):
        replace(successful_attempt(), feed_age_seconds=invalid_age)
