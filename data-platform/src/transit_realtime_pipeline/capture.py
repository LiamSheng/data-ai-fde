"""Fetch one named GTFS-Realtime feed and audit its outcome."""

import hashlib
import json
import os
import sys
import tempfile
from dataclasses import dataclass
from datetime import UTC, datetime
from functools import partial
from pathlib import Path, PurePosixPath
from uuid import uuid4

import httpx

from .config import FEED_TYPES, load_feed_config
from .gtfs_rt import InvalidFeed, parse_feed
from .models import CaptureAttempt, ErrorCategory, Outcome
from .storage import CaptureStorage


@dataclass(frozen=True, slots=True)
class CaptureResult:
    """One capture as poller.main needs it for the poll cycle record.

    attempt is the record held in memory; it is None only when even that
    could not be built. audit_written says whether its audit was persisted.
    """

    exit_code: int
    feed_type: str
    attempt_id: str
    started_at_utc: datetime
    attempt: CaptureAttempt | None
    audit_written: bool


def save_raw_payload(
    payload: bytes, storage_root: Path, feed_type: str = "vehicle_positions"
) -> tuple[str, str, str]:
    """Save local bytes without replacing an existing content-addressed file."""
    if feed_type not in FEED_TYPES:
        raise ValueError("unknown feed type")

    digest = hashlib.sha256(payload).hexdigest()
    payload_ref = PurePosixPath(feed_type, digest[:2], f"{digest}.pb")
    destination = storage_root.joinpath(*payload_ref.parts)
    destination.parent.mkdir(parents=True, exist_ok=True)

    descriptor, temporary_name = tempfile.mkstemp(
        prefix=".pending-", dir=destination.parent
    )

    try:
        with os.fdopen(descriptor, "wb") as temporary_file:
            temporary_file.write(payload)
            temporary_file.flush()
            os.fsync(temporary_file.fileno())

        # Linking creates the destination atomically without overwriting it.
        try:
            os.link(temporary_name, destination)
            disposition = "new"
        except FileExistsError:
            disposition = "reused"

        # Local mode verifies the existing bytes before reporting reuse.
        # GCS mode deliberately has a different, documented trust boundary.
        if destination.read_bytes() != payload:
            raise OSError("stored payload does not match the response")
    finally:
        Path(temporary_name).unlink(missing_ok=True)

    return payload_ref.as_posix(), digest, disposition


def write_attempt_record(attempt: CaptureAttempt, record_root: Path) -> Path:
    """Atomically create one local JSON record named by attempt_id."""
    record_root.mkdir(parents=True, exist_ok=True)
    destination = record_root / f"{attempt.attempt_id}.json"

    contents = (
        json.dumps(attempt.to_record(), indent=2, sort_keys=True, allow_nan=False)
        + "\n"
    ).encode("utf-8")

    descriptor, temporary_name = tempfile.mkstemp(prefix=".pending-", dir=record_root)

    try:
        with os.fdopen(descriptor, "wb") as temporary_file:
            temporary_file.write(contents)
            temporary_file.flush()
            os.fsync(temporary_file.fileno())

        # Never overwrite another attempt record.
        os.link(temporary_name, destination)
    finally:
        Path(temporary_name).unlink(missing_ok=True)

    return destination


def audit_failure(
    *,
    attempt_id: str,
    started_at: datetime,
    record_root: Path,
    storage: CaptureStorage | None = None,
    feed_type: str = "vehicle_positions",
    poll_cycle_id: str | None = None,
    outcome: Outcome,
    error_category: ErrorCategory,
    exit_code: int = 1,
    http_status: int | None = None,
    payload_ref: str | None = None,
    payload_sha256: str | None = None,
    payload_bytes: int | None = None,
    incrementality: str | None = None,
    entity_count: int | None = None,
    vehicle_count: int | None = None,
    trip_update_count: int | None = None,
    alert_count: int | None = None,
) -> CaptureResult:
    """Write a categorized failure without storing exception text or URL."""
    completed_at = datetime.now(UTC)

    # A rare wall-clock adjustment must not make the audit impossible
    # to serialize. Clock behavior needs separate operational monitoring.
    completed_at = max(completed_at, started_at)

    try:
        attempt = CaptureAttempt(
            attempt_id=attempt_id,
            poll_cycle_id=poll_cycle_id,
            source_id="primary",
            feed_type=feed_type,
            started_at_utc=started_at,
            completed_at_utc=completed_at,
            outcome=outcome,
            error_category=error_category,
            http_status=http_status,
            payload_ref=payload_ref,
            payload_sha256=payload_sha256,
            payload_bytes=payload_bytes,
            incrementality=incrementality,
            entity_count=entity_count,
            vehicle_count=vehicle_count,
            trip_update_count=trip_update_count,
            alert_count=alert_count,
        )
    except Exception:  # noqa: BLE001 - redact untrusted exception details
        # Without a record there is nothing to audit or to keep in memory.
        print("audit_unavailable", file=sys.stderr)
        return CaptureResult(3, feed_type, attempt_id, started_at, None, False)

    try:
        # Failures must use the same destination as successful captures.
        # Never silently fall back to local storage when GCS auditing fails.
        if storage is None:
            write_attempt_record(attempt, record_root)
            record_ref = f"data/ops/capture_attempts/{attempt.attempt_id}.json"
        else:
            record_ref = storage.write_attempt_record(attempt)

    except Exception:  # noqa: BLE001 - redact untrusted exception details
        # The audit sink cannot record its own unavailability there.
        # Raw exception text could contain a credential, so do not print it.
        # The poll cycle record keeps the in-memory attempt instead.
        print("audit_unavailable", file=sys.stderr)
        return CaptureResult(3, feed_type, attempt_id, started_at, attempt, False)

    print(f"feed_type={feed_type}")
    print(f"attempt_id={attempt_id}")
    print(f"outcome={outcome.value}")
    print(f"error_category={error_category.value}")

    if http_status is not None:
        print(f"http_status={http_status}")

    print(f"attempt_record={record_ref}")
    return CaptureResult(exit_code, feed_type, attempt_id, started_at, attempt, True)


def capture_one(
    feed_type: str,
    *,
    poll_cycle_id: str | None = None,
    storage: CaptureStorage | None = None,
) -> int:
    """Capture one feed and return only its exit code."""
    return capture_feed(
        feed_type, poll_cycle_id=poll_cycle_id, storage=storage
    ).exit_code


def capture_feed(
    feed_type: str,
    *,
    poll_cycle_id: str | None = None,
    storage: CaptureStorage | None = None,
) -> CaptureResult:
    """Capture one feed with local persistence or an injected storage adapter."""
    # An attempt begins before source configuration validation.
    # Configuration failures therefore get an attempt even without HTTP.
    attempt_id = uuid4().hex
    started_at = datetime.now(UTC)

    # These paths are used only in local mode.
    storage_root = Path.cwd() / "data/raw/gtfs_rt"
    record_root = Path.cwd() / "data/ops/capture_attempts"

    # Bind the selected destination once for every failure branch below.
    audit = partial(audit_failure, storage=storage)

    if storage is not None:
        # If audit persistence later fails, execution logs retain the identity.
        # The poller separately prints the shared poll_cycle_id.
        print(f"capture_started feed_type={feed_type} attempt_id={attempt_id}")

    try:
        config = load_feed_config(feed_type)
    except ValueError:
        return audit(
            attempt_id=attempt_id,
            started_at=started_at,
            record_root=record_root,
            feed_type=feed_type,
            poll_cycle_id=poll_cycle_id,
            outcome=Outcome.CONFIG_ERROR,
            error_category=ErrorCategory.CONFIGURATION,
            exit_code=2,
        )
    except Exception:  # noqa: BLE001 - redact untrusted exception details
        return audit(
            attempt_id=attempt_id,
            started_at=started_at,
            record_root=record_root,
            feed_type=feed_type,
            poll_cycle_id=poll_cycle_id,
            outcome=Outcome.OTHER_ERROR,
            error_category=ErrorCategory.OTHER,
        )

    try:
        with httpx.Client(
            timeout=20.0,
            follow_redirects=False,
            trust_env=False,
        ) as client:
            response = client.get(
                config.url,
                params={"apikey": config.api_key},
            )
    except httpx.TimeoutException:
        return audit(
            attempt_id=attempt_id,
            started_at=started_at,
            record_root=record_root,
            feed_type=feed_type,
            poll_cycle_id=poll_cycle_id,
            outcome=Outcome.TIMEOUT,
            error_category=ErrorCategory.TIMEOUT,
        )
    except (httpx.HTTPError, ValueError):
        # HTTP exceptions may contain the complete request URL and its key.
        return audit(
            attempt_id=attempt_id,
            started_at=started_at,
            record_root=record_root,
            feed_type=feed_type,
            poll_cycle_id=poll_cycle_id,
            outcome=Outcome.TRANSPORT_ERROR,
            error_category=ErrorCategory.TRANSPORT,
        )
    except Exception:  # noqa: BLE001 - redact untrusted exception details
        return audit(
            attempt_id=attempt_id,
            started_at=started_at,
            record_root=record_root,
            feed_type=feed_type,
            poll_cycle_id=poll_cycle_id,
            outcome=Outcome.OTHER_ERROR,
            error_category=ErrorCategory.OTHER,
        )

    status = response.status_code

    if not 200 <= status < 300:
        if status in (401, 403):
            category = ErrorCategory.AUTHENTICATION
        elif 400 <= status < 500:
            category = ErrorCategory.HTTP_CLIENT
        elif 500 <= status < 600:
            category = ErrorCategory.HTTP_SERVER
        else:
            # For example, an unfollowed redirect retains its exact status.
            category = ErrorCategory.OTHER

        return audit(
            attempt_id=attempt_id,
            started_at=started_at,
            record_root=record_root,
            feed_type=feed_type,
            poll_cycle_id=poll_cycle_id,
            outcome=Outcome.HTTP_ERROR,
            error_category=category,
            http_status=status,
        )

    try:
        payload = response.content
        payload_bytes = len(payload)
        digest = hashlib.sha256(payload).hexdigest()
    except Exception:  # noqa: BLE001 - redact untrusted exception details
        return audit(
            attempt_id=attempt_id,
            started_at=started_at,
            record_root=record_root,
            feed_type=feed_type,
            poll_cycle_id=poll_cycle_id,
            outcome=Outcome.OTHER_ERROR,
            error_category=ErrorCategory.OTHER,
            http_status=status,
        )

    # Preserve received bytes before parsing, including an empty HTTP body.
    # A valid zero-entity feed is different from an empty byte sequence.
    try:
        if storage is None:
            payload_ref, saved_digest, disposition = save_raw_payload(
                payload, storage_root, feed_type
            )
        else:
            payload_ref, saved_digest, disposition = storage.save_raw_payload(
                payload, feed_type
            )
    except OSError:
        return audit(
            attempt_id=attempt_id,
            started_at=started_at,
            record_root=record_root,
            feed_type=feed_type,
            poll_cycle_id=poll_cycle_id,
            outcome=Outcome.STORAGE_ERROR,
            error_category=ErrorCategory.STORAGE,
            http_status=status,
            payload_sha256=digest,
            payload_bytes=payload_bytes,
        )
    except Exception:  # noqa: BLE001 - redact untrusted exception details
        return audit(
            attempt_id=attempt_id,
            started_at=started_at,
            record_root=record_root,
            feed_type=feed_type,
            poll_cycle_id=poll_cycle_id,
            outcome=Outcome.OTHER_ERROR,
            error_category=ErrorCategory.OTHER,
            http_status=status,
            payload_sha256=digest,
            payload_bytes=payload_bytes,
        )

    # Verify the adapter's returned content identity.
    # In GCS "existing" mode, this does not verify the old object's bytes.
    if saved_digest != digest:
        return audit(
            attempt_id=attempt_id,
            started_at=started_at,
            record_root=record_root,
            feed_type=feed_type,
            poll_cycle_id=poll_cycle_id,
            outcome=Outcome.STORAGE_ERROR,
            error_category=ErrorCategory.STORAGE,
            http_status=status,
            payload_sha256=digest,
            payload_bytes=payload_bytes,
        )

    try:
        metadata = parse_feed(payload)
    except InvalidFeed:
        return audit(
            attempt_id=attempt_id,
            started_at=started_at,
            record_root=record_root,
            feed_type=feed_type,
            poll_cycle_id=poll_cycle_id,
            outcome=Outcome.INVALID_PROTOBUF,
            error_category=ErrorCategory.INVALID_PROTOBUF,
            http_status=status,
            payload_ref=payload_ref,
            payload_sha256=digest,
            payload_bytes=payload_bytes,
        )
    except Exception:  # noqa: BLE001 - redact untrusted exception details
        return audit(
            attempt_id=attempt_id,
            started_at=started_at,
            record_root=record_root,
            feed_type=feed_type,
            poll_cycle_id=poll_cycle_id,
            outcome=Outcome.OTHER_ERROR,
            error_category=ErrorCategory.OTHER,
            http_status=status,
            payload_ref=payload_ref,
            payload_sha256=digest,
            payload_bytes=payload_bytes,
        )

    if metadata.feed_timestamp is None:
        # Valid Protobuf can still fail our freshness-metadata contract.
        # Keep this separate from a decoding failure.
        return audit(
            attempt_id=attempt_id,
            started_at=started_at,
            record_root=record_root,
            feed_type=feed_type,
            poll_cycle_id=poll_cycle_id,
            outcome=Outcome.FEED_METADATA_ERROR,
            error_category=ErrorCategory.FEED_METADATA,
            http_status=status,
            payload_ref=payload_ref,
            payload_sha256=digest,
            payload_bytes=payload_bytes,
            incrementality=metadata.incrementality,
            entity_count=metadata.entity_count,
            vehicle_count=metadata.vehicle_count,
            trip_update_count=metadata.trip_update_count,
            alert_count=metadata.alert_count,
        )

    # A FULL_DATASET must contain only the expected entity type.
    # Empty feeds are allowed.
    # Differential deletion entities can omit the body, so they are excluded
    # from this snapshot-specific check.
    expected_count = {
        "vehicle_positions": metadata.vehicle_count,
        "trip_updates": metadata.trip_update_count,
        "service_alerts": metadata.alert_count,
    }[feed_type]

    if (
        metadata.incrementality == "FULL_DATASET"
        and expected_count != metadata.entity_count
    ):
        return audit(
            attempt_id=attempt_id,
            started_at=started_at,
            record_root=record_root,
            feed_type=feed_type,
            poll_cycle_id=poll_cycle_id,
            outcome=Outcome.FEED_CONTENT_ERROR,
            error_category=ErrorCategory.FEED_CONTENT,
            http_status=status,
            payload_ref=payload_ref,
            payload_sha256=digest,
            payload_bytes=payload_bytes,
            incrementality=metadata.incrementality,
            entity_count=metadata.entity_count,
            vehicle_count=metadata.vehicle_count,
            trip_update_count=metadata.trip_update_count,
            alert_count=metadata.alert_count,
        )

    completed_at = datetime.now(UTC)

    if completed_at < started_at:
        return audit(
            attempt_id=attempt_id,
            started_at=started_at,
            record_root=record_root,
            feed_type=feed_type,
            poll_cycle_id=poll_cycle_id,
            outcome=Outcome.OTHER_ERROR,
            error_category=ErrorCategory.OTHER,
            http_status=status,
            payload_ref=payload_ref,
            payload_sha256=digest,
            payload_bytes=payload_bytes,
        )

    try:
        attempt = CaptureAttempt(
            attempt_id=attempt_id,
            poll_cycle_id=poll_cycle_id,
            source_id="primary",
            feed_type=feed_type,
            started_at_utc=started_at,
            completed_at_utc=completed_at,
            outcome=Outcome.SUCCESS,
            http_status=status,
            payload_ref=payload_ref,
            payload_sha256=digest,
            payload_bytes=payload_bytes,
            feed_timestamp=metadata.feed_timestamp,
            feed_age_seconds=(completed_at - metadata.feed_timestamp).total_seconds(),
            incrementality=metadata.incrementality,
            entity_count=metadata.entity_count,
            vehicle_count=metadata.vehicle_count,
            trip_update_count=metadata.trip_update_count,
            alert_count=metadata.alert_count,
        )
    except Exception:  # noqa: BLE001 - redact untrusted exception details
        return audit(
            attempt_id=attempt_id,
            started_at=started_at,
            record_root=record_root,
            feed_type=feed_type,
            poll_cycle_id=poll_cycle_id,
            outcome=Outcome.OTHER_ERROR,
            error_category=ErrorCategory.OTHER,
            http_status=status,
            payload_ref=payload_ref,
            payload_sha256=digest,
            payload_bytes=payload_bytes,
        )

    try:
        if storage is None:
            write_attempt_record(attempt, record_root)
            record_ref = f"data/ops/capture_attempts/{attempt.attempt_id}.json"
        else:
            record_ref = storage.write_attempt_record(attempt)
    except Exception:  # noqa: BLE001 - redact untrusted exception details
        # The payload may already exist, but audit persistence has failed.
        # Do not report a successful capture or claim an audit record exists;
        # the poll cycle record keeps the in-memory attempt as secondary evidence.
        print("audit_unavailable", file=sys.stderr)
        return CaptureResult(3, feed_type, attempt_id, started_at, attempt, False)

    print(f"feed_type={feed_type}")
    print(f"attempt_id={attempt_id}")
    print(f"outcome={attempt.outcome.value}")
    print(f"payload_ref={payload_ref}")
    print(f"payload_sha256={digest}")
    print(f"payload_bytes={payload_bytes}")
    print(f"storage={disposition}")
    print(f"entity_count={metadata.entity_count}")
    print(f"feed_age_seconds={attempt.feed_age_seconds:.3f}")
    print(f"attempt_record={record_ref}")
    return CaptureResult(0, feed_type, attempt_id, started_at, attempt, True)


def main() -> int:
    """Keep the existing single-feed entry point in local mode.

    Use transit_realtime_pipeline.poller for backend selection and GCS mode.
    """
    return capture_one("vehicle_positions")


if __name__ == "__main__":
    raise SystemExit(main())
