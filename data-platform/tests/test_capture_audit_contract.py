"""The capture audit contract, from the producer's side.

The schemas and examples live in contracts/capture-audit/. Bronze runs the same
examples through its own checks in databricks/ingestion/tests/test_audit_contract.py.
"""

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import jsonschema
import pytest

from transit_realtime_pipeline.models import (
    AUDIT_SCHEMA_VERSION,
    CaptureAttempt,
    ErrorCategory,
    Outcome,
)
from transit_realtime_pipeline.schedule_models import (
    ScheduleAttempt,
    ScheduleErrorCategory,
    ScheduleOutcome,
)

CONTRACT = Path(__file__).resolve().parents[1] / "contracts" / "capture-audit"
EXAMPLES = CONTRACT / "examples"
INVALID = EXAMPLES / "invalid"
KINDS = ("capture_attempt", "schedule_capture_attempt")

STARTED = datetime(2026, 11, 5, 20, 0, 11, tzinfo=UTC)
COMPLETED = STARTED + timedelta(seconds=1)
DIGEST = "7552d42d911dfff5a8a183da0a12cf9c95bdfef396bb3e6815ecf4b6dd5952e7"
ATTEMPT_ID = "09d0252fe08e4096bcc352074df09085"


def schema(kind: str) -> dict:
    return json.loads((CONTRACT / f"{kind}.v1.schema.json").read_text())


def validator(kind: str) -> jsonschema.Draft202012Validator:
    return jsonschema.Draft202012Validator(
        schema(kind), format_checker=jsonschema.FormatChecker()
    )


def manifest() -> list[dict]:
    return json.loads((INVALID / "manifest.json").read_text())


def test_schemas_are_valid_json_schema() -> None:
    for kind in KINDS:
        jsonschema.Draft202012Validator.check_schema(schema(kind))


@pytest.mark.parametrize("path", sorted(EXAMPLES.glob("*.json")), ids=lambda p: p.name)
def test_every_valid_example_matches_its_schema(path: Path) -> None:
    validator(path.name.split(".")[0]).validate(json.loads(path.read_text()))


@pytest.mark.parametrize("case", manifest(), ids=lambda case: case["file"])
def test_invalid_examples_are_rejected_unless_only_bronze_can_tell(case: dict) -> None:
    document = json.loads((INVALID / case["file"]).read_text())
    errors = list(validator(case["schema"]).iter_errors(document))
    # schema_valid: true marks rules JSON Schema cannot express (see README).
    assert (not errors) is case["schema_valid"], [error.message for error in errors]


def test_every_invalid_example_is_listed_in_the_manifest() -> None:
    listed = {case["file"] for case in manifest()}
    on_disk = {path.name for path in INVALID.glob("*.json")} - {"manifest.json"}
    assert listed == on_disk


# The schema and the code must name the same values; drift here is the failure
# mode a shared contract exists to catch.


def test_realtime_enums_match_the_model() -> None:
    properties = schema("capture_attempt")["properties"]
    assert set(properties["outcome"]["enum"]) == {item.value for item in Outcome}
    assert set(properties["error_category"]["enum"]) == {
        item.value for item in ErrorCategory
    }


def test_schedule_enums_match_the_model() -> None:
    properties = schema("schedule_capture_attempt")["properties"]
    assert set(properties["outcome"]["enum"]) == {
        item.value for item in ScheduleOutcome
    }
    assert set(properties["error_category"]["enum"]) == {
        item.value for item in ScheduleErrorCategory
    }


def test_schema_version_matches_the_model() -> None:
    for kind in KINDS:
        assert schema(kind)["properties"]["schema_version"]["const"] == (
            AUDIT_SCHEMA_VERSION
        )


# Every outcome the collectors can write produces a document the schema accepts.

FAILURES = {
    Outcome.CONFIG_ERROR: (ErrorCategory.CONFIGURATION, None),
    Outcome.HTTP_ERROR: (ErrorCategory.HTTP_SERVER, 500),
    Outcome.TIMEOUT: (ErrorCategory.TIMEOUT, None),
    Outcome.INVALID_PROTOBUF: (ErrorCategory.INVALID_PROTOBUF, 200),
    Outcome.FEED_METADATA_ERROR: (ErrorCategory.FEED_METADATA, 200),
    Outcome.FEED_CONTENT_ERROR: (ErrorCategory.FEED_CONTENT, 200),
    Outcome.TRANSPORT_ERROR: (ErrorCategory.TRANSPORT, None),
    Outcome.STORAGE_ERROR: (ErrorCategory.STORAGE, 200),
    Outcome.OTHER_ERROR: (ErrorCategory.OTHER, None),
}


def realtime_attempt(outcome: Outcome) -> CaptureAttempt:
    common = {
        "attempt_id": ATTEMPT_ID,
        "poll_cycle_id": "360c9ee9a47748bfbe7f3b87ee369a12",
        "source_id": "primary",
        "feed_type": "trip_updates",
        "started_at_utc": STARTED,
        "completed_at_utc": COMPLETED,
        "outcome": outcome,
    }
    if outcome is Outcome.SUCCESS:
        return CaptureAttempt(
            **common,
            http_status=200,
            payload_ref=f"trip_updates/{DIGEST[:2]}/{DIGEST}.pb",
            payload_sha256=DIGEST,
            payload_bytes=683343,
            feed_timestamp=STARTED - timedelta(seconds=14),
            feed_age_seconds=15.0,
            incrementality="FULL_DATASET",
            entity_count=754,
            trip_update_count=754,
        )
    category, status = FAILURES[outcome]
    return CaptureAttempt(**common, http_status=status, error_category=category)


def test_failure_table_covers_every_outcome() -> None:
    assert set(FAILURES) | {Outcome.SUCCESS} == set(Outcome)


@pytest.mark.parametrize("outcome", list(Outcome), ids=str)
def test_every_realtime_outcome_writes_a_valid_audit(outcome: Outcome) -> None:
    validator("capture_attempt").validate(realtime_attempt(outcome).to_record())


@pytest.mark.parametrize("outcome", list(ScheduleOutcome), ids=str)
def test_every_schedule_outcome_writes_a_valid_audit(outcome: ScheduleOutcome) -> None:
    common = {
        "attempt_id": ATTEMPT_ID,
        "started_at_utc": STARTED,
        "completed_at_utc": COMPLETED,
        "outcome": outcome,
    }
    if outcome is ScheduleOutcome.SUCCESS:
        attempt = ScheduleAttempt(
            **common,
            http_status=200,
            payload_ref=f"gtfs_schedule/{DIGEST[:2]}/{DIGEST}.zip",
            payload_sha256=DIGEST,
            payload_bytes=21734812,
            storage_disposition="new",
            archive_files=("agency.txt", "routes.txt"),
            feed_start_date="20261025",
        )
    else:
        category = ScheduleErrorCategory(
            {
                "config_error": "configuration",
                "http_error": "http_server",
                "transport_error": "transport",
                "storage_error": "storage",
                "other_error": "other",
            }.get(outcome.value, outcome.value)
        )
        status = 503 if outcome is ScheduleOutcome.HTTP_ERROR else None
        attempt = ScheduleAttempt(**common, http_status=status, error_category=category)
    validator("schedule_capture_attempt").validate(attempt.to_record())


def test_the_real_success_example_is_what_the_model_writes() -> None:
    # The example is a real audit; the model rebuilt from it writes it back exactly.
    example = json.loads((EXAMPLES / "capture_attempt.success.json").read_text())
    rebuilt = CaptureAttempt(
        attempt_id=example["attempt_id"],
        poll_cycle_id=example["poll_cycle_id"],
        source_id=example["source_id"],
        feed_type=example["feed_type"],
        started_at_utc=datetime.fromisoformat(example["started_at_utc"]),
        completed_at_utc=datetime.fromisoformat(example["completed_at_utc"]),
        outcome=Outcome(example["outcome"]),
        http_status=example["http_status"],
        payload_ref=example["payload_ref"],
        payload_sha256=example["payload_sha256"],
        payload_bytes=example["payload_bytes"],
        feed_timestamp=datetime.fromisoformat(example["feed_timestamp"]),
        feed_age_seconds=example["feed_age_seconds"],
        incrementality=example["incrementality"],
        entity_count=example["entity_count"],
        vehicle_count=example["vehicle_count"],
        trip_update_count=example["trip_update_count"],
        alert_count=example["alert_count"],
    )
    assert rebuilt.to_record() == example
