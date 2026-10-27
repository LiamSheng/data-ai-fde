"""Tests for the capture attempt audit model."""

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


def successful_attempt(attempt_id: str) -> CaptureAttempt:
    """Build a complete, synthetic success record for model tests."""
    return CaptureAttempt(
        attempt_id=attempt_id,
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


def test_two_attempts_can_reference_the_same_payload() -> None:
    first = successful_attempt("attempt-001")
    second = successful_attempt("attempt-002")

    assert first.attempt_id != second.attempt_id
    assert first.payload_ref == second.payload_ref
    assert first.payload_sha256 == second.payload_sha256

    # A valid empty feed is different from a failed request.
    assert first.entity_count == 0
    assert first.outcome is Outcome.SUCCESS


def test_http_401_can_be_recorded_without_a_payload() -> None:
    attempt = CaptureAttempt(
        attempt_id="attempt-003",
        source_id="primary",
        feed_type="vehicle_positions",
        started_at_utc=STARTED,
        completed_at_utc=STARTED + timedelta(seconds=1),
        outcome=Outcome.HTTP_ERROR,
        http_status=401,
        error_category=ErrorCategory.AUTHENTICATION,
    )

    assert attempt.http_status == 401
    assert attempt.payload_ref is None
    assert attempt.error_category is ErrorCategory.AUTHENTICATION


def test_storage_failure_cannot_be_reported_as_success() -> None:
    failed = CaptureAttempt(
        attempt_id="attempt-004",
        source_id="primary",
        feed_type="vehicle_positions",
        started_at_utc=STARTED,
        completed_at_utc=STARTED + timedelta(seconds=1),
        outcome=Outcome.STORAGE_ERROR,
        http_status=200,
        payload_sha256=DIGEST,
        error_category=ErrorCategory.STORAGE,
    )

    assert failed.outcome is Outcome.STORAGE_ERROR
    assert failed.payload_ref is None

    # Removing the saved-file reference invalidates an otherwise complete success.
    with pytest.raises(ValueError, match="success requires"):
        replace(successful_attempt("attempt-005"), payload_ref=None)


def test_raw_error_text_is_rejected_without_echoing_it() -> None:
    fake_key = "test-secret-value"
    raw_error = f"request failed with credential {fake_key}"

    with pytest.raises(ValueError, match="known category") as error:
        CaptureAttempt(
            attempt_id="attempt-006",
            source_id="primary",
            feed_type="vehicle_positions",
            started_at_utc=STARTED,
            completed_at_utc=STARTED + timedelta(seconds=1),
            outcome=Outcome.HTTP_ERROR,
            http_status=401,
            error_category=raw_error,  # type: ignore[arg-type]
        )

    assert fake_key not in str(error.value)


@pytest.mark.parametrize(
    "payload_ref",
    ["/absolute/payload.pb", "cloud://bucket/payload.pb"],
)
def test_payload_ref_must_be_relative(payload_ref: str) -> None:
    with pytest.raises(ValueError, match="relative object reference"):
        replace(successful_attempt("attempt-007"), payload_ref=payload_ref)


def test_success_rejects_empty_incrementality() -> None:
    # An empty string is present, but it is not meaningful feed metadata.
    with pytest.raises(ValueError, match="incrementality"):
        replace(successful_attempt("attempt-008"), incrementality="")
