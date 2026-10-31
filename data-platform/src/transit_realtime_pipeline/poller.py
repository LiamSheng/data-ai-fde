"""Collect the requested feeds once, then record the cycle and the latest status.

Write order (contracts/landing-status/README.md): each feed's own audit during
capture, then the poll cycle record (strict: a failure exits with 3), then
latest.json (best-effort: a failure is logged and never changes the exit code).
"""

import os
import re
import sys
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from .capture import CaptureResult, capture_feed
from .config import FEED_TYPES
from .status import CycleFeedAttempt, LatestStatus, PollCycleRecord
from .storage import (
    CaptureStorage,
    configured_cloud_storage,
    configured_latest_status_store,
    write_local_poll_cycle_record,
)

# Comma-separated subset of FEED_TYPES, in capture order; unset means all three.
REQUESTED_FEEDS_ENV = "GTFS_RT_REQUESTED_FEEDS"
# Injected by Cloud Run for every job execution.
EXECUTION_NAME_ENV = "CLOUD_RUN_EXECUTION"
# Set in the runtime image from the build's SOURCE_COMMIT.
COLLECTOR_REVISION_ENV = "COLLECTOR_REVISION"

_REVISION = re.compile(r"[0-9a-f]{40}")


def requested_feeds() -> tuple[str, ...]:
    """The feeds this execution should capture; raises on an invalid selection."""
    raw = os.getenv(REQUESTED_FEEDS_ENV)
    if raw is None:
        return FEED_TYPES
    feeds = tuple(part.strip() for part in raw.split(","))
    if any(feed not in FEED_TYPES for feed in feeds) or len(set(feeds)) != len(feeds):
        raise ValueError("invalid feed selection")
    return feeds


def execution_name() -> str | None:
    return os.getenv(EXECUTION_NAME_ENV) or None


def collector_revision() -> str | None:
    """A full commit SHA, or None for local runs and unrecorded builds."""
    value = os.getenv(COLLECTOR_REVISION_ENV)
    if value is None or not _REVISION.fullmatch(value):
        return None
    return value


def record_poll_cycle(
    *,
    poll_cycle_id: str,
    started_at: datetime,
    feeds: tuple[str, ...],
    results: list[CaptureResult],
    storage: CaptureStorage | None,
) -> tuple[int, PollCycleRecord | None, str | None]:
    """Write the cycle record. Returns (exit code, record, its name if written)."""
    # A wall-clock step backwards must not make the record impossible to build.
    completed_at = max(datetime.now(UTC), started_at)
    try:
        cycle = PollCycleRecord(
            poll_cycle_id=poll_cycle_id,
            started_at_utc=started_at,
            completed_at_utc=completed_at,
            requested_feeds=feeds,
            execution_name=execution_name(),
            collector_revision=collector_revision(),
            feeds=tuple(
                CycleFeedAttempt.from_capture(
                    feed_type=result.feed_type,
                    attempt_id=result.attempt_id,
                    started_at_utc=result.started_at_utc,
                    attempt=result.attempt,
                    audit_written=result.audit_written,
                )
                for result in results
            ),
        )
    except Exception:  # noqa: BLE001 - a contradictory record is never written
        print("poll_cycle_record_invalid", file=sys.stderr)
        return 3, None, None

    name = cycle.object_name
    try:
        if storage is None:
            write_local_poll_cycle_record(Path.cwd(), name, cycle.to_json())
        else:
            storage.write_poll_cycle_record(name, cycle.to_json())
    except Exception:  # noqa: BLE001 - redact untrusted exception details
        print("poll_cycle_record_unavailable", file=sys.stderr)
        return 3, cycle, None

    print(f"poll_cycle_record={name}")
    return 0, cycle, name


def publish_latest_status(cycle: PollCycleRecord | None, cycle_ref: str | None) -> str:
    """Best-effort: every failure is reported, none is raised. Returns the result."""
    if cycle is None:
        result = "skipped_no_cycle_record"
    else:
        try:
            result = _publish(cycle, cycle_ref)
        except Exception:  # noqa: BLE001 - redact untrusted exception details
            result = "unavailable"
    stream = sys.stdout if result in ("written", "skipped_not_newer") else sys.stderr
    print(f"latest_status={result}", file=stream)
    return result


def _publish(cycle: PollCycleRecord, cycle_ref: str | None) -> str:
    store = configured_latest_status_store()
    if store is None:
        return "unconfigured"

    # A read error skips this cycle's write: starting from empty would erase
    # the carried values. A missing or malformed file starts from empty.
    contents = store.read()
    previous = None
    if contents is not None:
        try:
            previous = LatestStatus.from_json(contents)
        except Exception:  # noqa: BLE001 - any unparsable file starts from empty
            print("latest_status_previous_malformed", file=sys.stderr)

    latest = LatestStatus.derive(previous, cycle, cycle_ref)
    if latest is None:
        return "skipped_not_newer"
    store.write(latest.to_json())
    return "written"


def main() -> int:
    poll_cycle_id = uuid4().hex
    started_at = datetime.now(UTC)
    print(f"poll_cycle_id={poll_cycle_id}")

    try:
        feeds = requested_feeds()
    except ValueError:
        # A deployment error: fail before any source request.
        print("cycle_outcome=invalid_feed_selection", file=sys.stderr)
        return 2

    try:
        # Initialize once, then share the same storage across the feeds.
        storage = configured_cloud_storage()
    except Exception:  # noqa: BLE001 - redact credential/SDK exception details
        # No feed attempt has begun.
        # If persistence cannot initialize, no durable audit is promised.
        print("cycle_outcome=storage_initialization_error", file=sys.stderr)
        return 3

    results: list[CaptureResult] = []
    for feed_type in feeds:
        if storage is None:
            # Preserve the existing local calling convention.
            result = capture_feed(feed_type, poll_cycle_id=poll_cycle_id)
        else:
            result = capture_feed(
                feed_type,
                poll_cycle_id=poll_cycle_id,
                storage=storage,
            )

        # A failed feed does not stop the remaining feeds.
        results.append(result)

    outcomes = [result.exit_code for result in results]
    failures = sum(code != 0 for code in outcomes)
    print(f"feeds_attempted={len(outcomes)}")
    print(f"feeds_failed={failures}")

    cycle_code, cycle, cycle_ref = record_poll_cycle(
        poll_cycle_id=poll_cycle_id,
        started_at=started_at,
        feeds=feeds,
        results=results,
        storage=storage,
    )
    publish_latest_status(cycle, cycle_ref)

    # Preserve the most serious result, including a missing audit or cycle record.
    return max([*outcomes, cycle_code])


if __name__ == "__main__":
    raise SystemExit(main())
