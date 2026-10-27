"""Two synthetic captures against real GCS; no live transit API is called.

The HTTP source is mocked, but capture_one and GCSStorage are unchanged.
All objects live beneath validation/gcs_repeat/<check_id>/ so synthetic audit
records cannot be mistaken for production captures. The collector needs only
its existing objectCreator grant; independent object reads use the operator.
"""

import argparse
import hashlib
import json
import os
import re
import sys
from datetime import UTC, datetime
from typing import Any
from unittest.mock import patch
from uuid import uuid4

import httpx
from google.cloud import storage as gcs_sdk
from google.transit import gtfs_realtime_pb2

from transit_realtime_pipeline import capture
from transit_realtime_pipeline.models import CaptureAttempt
from transit_realtime_pipeline.storage import GCSStorage

SYNTHETIC_KEY = "synthetic-validation-key-not-a-credential"
SYNTHETIC_URL = "https://example.invalid/vehicle_positions"


class CheckFailed(Exception):
    """Only fixed, safe categories are returned to execution logs."""


def require(condition: bool, category: str) -> None:
    # Unlike assert, this validation remains active with python -O.
    if not condition:
        raise CheckFailed(category)


def make_payload(check_id: str, timestamp: int) -> bytes:
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    feed.header.incrementality = gtfs_realtime_pb2.FeedHeader.FULL_DATASET
    feed.header.timestamp = timestamp
    entity = feed.entity.add()
    entity.id = f"synthetic-{check_id}"
    entity.vehicle.vehicle.id = "synthetic-vehicle"
    entity.vehicle.position.latitude = 49.0
    entity.vehicle.position.longitude = -123.0
    require(feed.IsInitialized(), "synthetic_payload_invalid")
    return feed.SerializeToString(deterministic=True)


class NamespacedBlob:
    def __init__(self, blob: Any, name: str, receipts: list[dict]) -> None:
        self.blob = blob
        self.name = name
        self.receipts = receipts

    def upload_from_string(self, data: bytes, **kwargs: Any) -> None:
        require(kwargs.get("if_generation_match") == 0, "create_only_missing")
        self.blob.upload_from_string(data, **kwargs)
        # upload_from_string populates Blob.generation from the write response.
        # No collector-side read/list/download request is made.
        self.receipts.append(
            {"name": self.name, "generation": str(self.blob.generation)}
        )


class NamespacedBucket:
    def __init__(self, bucket: Any, prefix: str, receipts: list[dict]) -> None:
        self.bucket = bucket
        self.prefix = prefix
        self.receipts = receipts

    def blob(self, name: str) -> NamespacedBlob:
        allowed = re.fullmatch(
            r"vehicle_positions/[0-9a-f]{2}/[0-9a-f]{64}\.pb"
            r"|audit/capture_attempts/[0-9a-f]{32}\.json",
            name,
        )
        require(allowed is not None, "unexpected_object_reference")
        physical_name = self.prefix + name
        return NamespacedBlob(
            self.bucket.blob(physical_name), physical_name, self.receipts
        )


class NamespacedClient:
    def __init__(self, client: Any, prefix: str) -> None:
        self.client = client
        self.prefix = prefix
        self.receipts: list[dict] = []

    def bucket(self, name: str) -> NamespacedBucket:
        return NamespacedBucket(self.client.bucket(name), self.prefix, self.receipts)


class RecordingStorage:
    """Observe successful adapter returns without changing their semantics."""

    def __init__(self, delegate: GCSStorage) -> None:
        self.delegate = delegate
        self.payload_results: list[tuple[str, str, str]] = []
        self.attempts: list[CaptureAttempt] = []

    def save_raw_payload(self, payload: bytes, feed_type: str) -> tuple[str, str, str]:
        result = self.delegate.save_raw_payload(payload, feed_type)
        self.payload_results.append(result)
        return result

    def write_attempt_record(self, attempt: CaptureAttempt) -> str:
        reference = self.delegate.write_attempt_record(attempt)
        self.attempts.append(attempt)
        return reference


def run_check(bucket: str, check_id: str, *, client: Any | None = None) -> dict:
    require(re.fullmatch(r"[0-9a-f]{32}", check_id) is not None, "invalid_check_id")
    require(
        re.fullmatch(r"[a-z0-9][a-z0-9._-]{1,220}[a-z0-9]", bucket) is not None,
        "invalid_bucket",
    )
    prefix = f"validation/gcs_repeat/{check_id}/"
    print(f"verification_id={check_id}")
    print(f"verification_prefix={prefix}")
    print("http_source=synthetic_mock")
    if client is None:
        client = gcs_sdk.Client()
    scoped_client = NamespacedClient(client, prefix)
    sink = RecordingStorage(GCSStorage(bucket, client=scoped_client))
    payload = make_payload(check_id, int(datetime.now(UTC).timestamp()))
    digest = hashlib.sha256(payload).hexdigest()
    cycle_ids = [uuid4().hex, uuid4().hex]
    requests_seen = 0

    def respond(request: httpx.Request) -> httpx.Response:
        nonlocal requests_seen
        require(
            request.url.host == "example.invalid" and request.method == "GET",
            "unexpected_source_request",
        )
        requests_seen += 1
        return httpx.Response(200, content=payload)

    real_http_client = httpx.Client
    transport = httpx.MockTransport(respond)

    def mocked_client(*args: Any, **kwargs: Any) -> httpx.Client:
        return real_http_client(*args, transport=transport, **kwargs)

    # This patches only this process. No real secret is read, printed or sent.
    source_environment = {
        "GTFS_RT_API_KEY": SYNTHETIC_KEY,
        "GTFS_RT_VEHICLE_POSITIONS_URL": SYNTHETIC_URL,
    }
    with (
        patch.dict(os.environ, source_environment),
        patch.object(capture.httpx, "Client", side_effect=mocked_client),
    ):
        for index, cycle_id in enumerate(cycle_ids, start=1):
            print(f"verification_capture={index} poll_cycle_id={cycle_id}")
            result = capture.capture_one(
                "vehicle_positions", poll_cycle_id=cycle_id, storage=sink
            )
            require(result == 0, f"capture_{index}_failed")

    require(requests_seen == 2, "unexpected_request_count")
    require(len(sink.payload_results) == 2, "unexpected_payload_result_count")
    require(
        [row[2] for row in sink.payload_results] == ["new", "existing"],
        "unexpected_storage_dispositions",
    )
    expected_ref = f"vehicle_positions/{digest[:2]}/{digest}.pb"
    require(
        all(row[:2] == (expected_ref, digest) for row in sink.payload_results),
        "payload_identity_mismatch",
    )
    require(len(sink.attempts) == 2, "unexpected_audit_count")
    require(
        len({attempt.attempt_id for attempt in sink.attempts}) == 2,
        "attempt_identity_collision",
    )
    require(
        [attempt.poll_cycle_id for attempt in sink.attempts] == cycle_ids,
        "cycle_identity_mismatch",
    )
    require(
        all(
            attempt.payload_ref == expected_ref
            and attempt.payload_sha256 == digest
            and attempt.payload_bytes == len(payload)
            for attempt in sink.attempts
        ),
        "audit_payload_mismatch",
    )
    payload_receipts = [
        row for row in scoped_client.receipts if row["name"].endswith(".pb")
    ]
    require(len(payload_receipts) == 1, "unexpected_payload_create_count")
    require(payload_receipts[0]["generation"] != "None", "generation_missing")
    require(len(scoped_client.receipts) == 3, "unexpected_successful_write_count")
    # These counts are successful write receipts, not a remote bucket listing.
    # The operator must independently read the objects before final acceptance.
    return {
        "verification_status": "application_checks_passed",
        "verification_id": check_id,
        "verification_prefix": prefix,
        "storage_dispositions": ["new", "existing"],
        "mock_http_requests": requests_seen,
        "successful_payload_creates": len(payload_receipts),
        "successful_audit_creates": len(sink.attempts),
        "payload_ref": expected_ref,
        "payload_sha256": digest,
        "payload_bytes": len(payload),
        "created_payload_generation": payload_receipts[0]["generation"],
        "poll_cycle_ids": cycle_ids,
        "attempt_ids": [attempt.attempt_id for attempt in sink.attempts],
        "independent_object_verification": "pending",
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--check-id", required=True)
    args = parser.parse_args(argv)
    try:
        report = run_check(args.bucket, args.check_id)
    except CheckFailed as error:
        print(f"verification_failed={error}", file=sys.stderr)
        return 1
    except Exception:  # noqa: BLE001 - SDK errors may contain sensitive details
        print("verification_failed=runtime_or_storage", file=sys.stderr)
        return 1
    print("verification_report=" + json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
