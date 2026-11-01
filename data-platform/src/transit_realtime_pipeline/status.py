"""Poll cycle records and the latest Landing status (contract v1).

The contract lives in contracts/landing-status/. Both documents are built only
through these classes. Their __post_init__ rejects what the contract forbids,
including the cross-field rules a JSON Schema cannot express, so a
contradictory document is never written.
"""

import json
import math
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from types import MappingProxyType
from typing import Any

from .config import FEED_TYPES
from .models import CaptureAttempt, ErrorCategory, Outcome, _require_utc, _utc_text

SCHEMA_VERSION = 1

# One poll interval (10 minutes) plus 5 minutes for a slow start or a long cycle.
# Revisit with time-of-day cadence (KAN-26). Readers use stale_after_utc as
# written, so files from an earlier setting stay readable.
STALE_AFTER = timedelta(seconds=900)

AUDIT_WRITTEN = "written"
AUDIT_FAILED = "failed"
AUDIT_STATUSES = (AUDIT_WRITTEN, AUDIT_FAILED)

AUDIT_PREFIX = "audit/capture_attempts/"
CYCLE_PREFIX = "audit/poll_cycles/"

_HEX32 = re.compile(r"[0-9a-f]{32}")
_HEX40 = re.compile(r"[0-9a-f]{40}")
_HEX64 = re.compile(r"[0-9a-f]{64}")
_UTC_TEXT = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,6})?Z")


# Shared checks and conversions.


def _require_hex(value: object, pattern: re.Pattern[str], name: str) -> None:
    if not isinstance(value, str) or not pattern.fullmatch(value):
        raise ValueError(f"{name} has an invalid format")


def _require_optional_utc(value: datetime | None) -> None:
    if value is not None:
        _require_utc(value)


def _text(value: datetime | None) -> str | None:
    return None if value is None else _utc_text(value)


def _parse_utc(value: object, name: str) -> datetime:
    if not isinstance(value, str) or not _UTC_TEXT.fullmatch(value):
        raise ValueError(f"{name} must be an RFC 3339 UTC timestamp")
    parsed = datetime.fromisoformat(value)
    _require_utc(parsed)
    return parsed


def _parse_optional_utc(value: object, name: str) -> datetime | None:
    return None if value is None else _parse_utc(value, name)


def _require_keys(record: object, names: tuple[str, ...], what: str) -> dict:
    """Every contract field is present, and no other field is."""
    if not isinstance(record, dict) or set(record) != set(names):
        raise ValueError(f"{what} does not have exactly the contract fields")
    return record


def _optional_int(value: object, name: str) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an integer")
    return value


def _optional_number(value: object, name: str) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{name} must be a number")
    return float(value)


def _to_json(record: dict[str, Any]) -> bytes:
    return (json.dumps(record, indent=2, allow_nan=False) + "\n").encode("utf-8")


def _check_capture_fields(
    *,
    audit_status: str,
    attempt_completed_at_utc: datetime | None,
    outcome: Outcome | None,
    error_category: ErrorCategory | None,
    http_status: int | None,
    feed_timestamp: datetime | None,
    feed_age_seconds: float | None,
    entity_count: int | None,
    success_fields: tuple[object, ...],
) -> None:
    """Rules shared by a cycle feed entry and the public last_attempt."""
    if audit_status not in AUDIT_STATUSES:
        raise ValueError("audit_status must be 'written' or 'failed'")
    if outcome is not None and not isinstance(outcome, Outcome):
        raise TypeError("outcome must be a known category")
    if error_category is not None and not isinstance(error_category, ErrorCategory):
        raise TypeError("error_category must be a known category")
    _require_optional_utc(attempt_completed_at_utc)
    _require_optional_utc(feed_timestamp)
    if http_status is not None and not 100 <= http_status <= 599:
        raise ValueError("http_status must be a valid HTTP status")
    if entity_count is not None and entity_count < 0:
        raise ValueError("entity_count cannot be negative")
    if feed_age_seconds is not None and not math.isfinite(feed_age_seconds):
        raise ValueError("feed_age_seconds must be finite")

    if outcome is None:
        # The attempt record could not even be built: nothing else is known.
        if audit_status != AUDIT_FAILED:
            raise ValueError("an attempt without an outcome cannot have an audit")
        if attempt_completed_at_utc is not None or error_category is not None:
            raise ValueError("an attempt without an outcome has no other values")
        if any(value is not None for value in success_fields):
            raise ValueError("an attempt without an outcome has no other values")
    elif outcome is Outcome.SUCCESS:
        if error_category is not None:
            raise ValueError("success cannot have an error_category")
        if http_status is None or not 200 <= http_status < 300:
            raise ValueError("success requires an HTTP 2xx status")
        if any(value is None for value in success_fields):
            raise ValueError("success requires the parsed feed values")
    elif error_category is None:
        raise ValueError("a failed attempt requires an error_category")

    if audit_status == AUDIT_WRITTEN and (
        outcome is None or attempt_completed_at_utc is None
    ):
        raise ValueError("a written audit has an outcome and a completion time")


# Poll cycle record.


@dataclass(frozen=True, slots=True)
class CycleFeedAttempt:
    """One feed attempt as the collector held it in memory.

    With audit_status 'failed' the collector could not confirm the audit
    write (an upload that timed out may still have landed). The values are
    secondary evidence, and no audit file is written afterwards.
    """

    feed_type: str
    attempt_id: str
    audit_status: str
    audit_ref: str | None
    attempt_started_at_utc: datetime
    attempt_completed_at_utc: datetime | None
    outcome: Outcome | None
    error_category: ErrorCategory | None
    http_status: int | None
    payload_ref: str | None
    payload_sha256: str | None
    feed_timestamp: datetime | None
    feed_age_seconds: float | None
    entity_count: int | None

    FIELDS = (
        "feed_type",
        "attempt_id",
        "audit_status",
        "audit_ref",
        "attempt_started_at_utc",
        "attempt_completed_at_utc",
        "outcome",
        "error_category",
        "http_status",
        "payload_ref",
        "payload_sha256",
        "feed_timestamp",
        "feed_age_seconds",
        "entity_count",
    )

    def __post_init__(self) -> None:
        if self.feed_type not in FEED_TYPES:
            raise ValueError("feed_type must be a known feed")
        _require_hex(self.attempt_id, _HEX32, "attempt_id")
        _require_utc(self.attempt_started_at_utc)
        _check_capture_fields(
            audit_status=self.audit_status,
            attempt_completed_at_utc=self.attempt_completed_at_utc,
            outcome=self.outcome,
            error_category=self.error_category,
            http_status=self.http_status,
            feed_timestamp=self.feed_timestamp,
            feed_age_seconds=self.feed_age_seconds,
            entity_count=self.entity_count,
            success_fields=(
                self.payload_ref,
                self.payload_sha256,
                self.feed_timestamp,
                self.feed_age_seconds,
                self.entity_count,
            ),
        )
        if (
            self.attempt_completed_at_utc is not None
            and self.attempt_completed_at_utc < self.attempt_started_at_utc
        ):
            raise ValueError("an attempt cannot complete before it starts")

        # Rule: audit_ref names this attempt, and exists only for a written audit.
        expected_ref = f"{AUDIT_PREFIX}{self.attempt_id}.json"
        if self.audit_status == AUDIT_WRITTEN and self.audit_ref != expected_ref:
            raise ValueError("audit_ref must name this attempt's audit")
        if self.audit_status == AUDIT_FAILED and self.audit_ref is not None:
            raise ValueError("a failed audit has no audit_ref")

        # Rule: payload_ref starts with the feed type and the digest prefix.
        if self.payload_sha256 is not None:
            _require_hex(self.payload_sha256, _HEX64, "payload_sha256")
        if self.payload_ref is not None:
            digest = self.payload_sha256
            if (
                digest is None
                or self.payload_ref != f"{self.feed_type}/{digest[:2]}/{digest}.pb"
            ):
                raise ValueError("payload_ref must match feed_type and payload_sha256")

    @classmethod
    def from_capture(
        cls,
        *,
        feed_type: str,
        attempt_id: str,
        started_at_utc: datetime,
        attempt: CaptureAttempt | None,
        audit_written: bool,
    ) -> "CycleFeedAttempt":
        """Summarize one capture. attempt is None when it could not be built."""
        if attempt is None:
            if audit_written:
                raise ValueError("an audit cannot be written without an attempt")
            return cls(
                feed_type=feed_type,
                attempt_id=attempt_id,
                audit_status=AUDIT_FAILED,
                audit_ref=None,
                attempt_started_at_utc=started_at_utc,
                attempt_completed_at_utc=None,
                outcome=None,
                error_category=None,
                http_status=None,
                payload_ref=None,
                payload_sha256=None,
                feed_timestamp=None,
                feed_age_seconds=None,
                entity_count=None,
            )
        if (
            attempt.feed_type != feed_type
            or attempt.attempt_id != attempt_id
            or attempt.started_at_utc != started_at_utc
        ):
            raise ValueError("the attempt does not belong to this capture")
        return cls(
            feed_type=feed_type,
            attempt_id=attempt_id,
            audit_status=AUDIT_WRITTEN if audit_written else AUDIT_FAILED,
            audit_ref=f"{AUDIT_PREFIX}{attempt_id}.json" if audit_written else None,
            attempt_started_at_utc=attempt.started_at_utc,
            attempt_completed_at_utc=attempt.completed_at_utc,
            outcome=attempt.outcome,
            error_category=attempt.error_category,
            http_status=attempt.http_status,
            payload_ref=attempt.payload_ref,
            payload_sha256=attempt.payload_sha256,
            feed_timestamp=attempt.feed_timestamp,
            feed_age_seconds=attempt.feed_age_seconds,
            entity_count=attempt.entity_count,
        )

    def to_record(self) -> dict[str, Any]:
        return {
            "feed_type": self.feed_type,
            "attempt_id": self.attempt_id,
            "audit_status": self.audit_status,
            "audit_ref": self.audit_ref,
            "attempt_started_at_utc": _utc_text(self.attempt_started_at_utc),
            "attempt_completed_at_utc": _text(self.attempt_completed_at_utc),
            "outcome": None if self.outcome is None else self.outcome.value,
            "error_category": (
                None if self.error_category is None else self.error_category.value
            ),
            "http_status": self.http_status,
            "payload_ref": self.payload_ref,
            "payload_sha256": self.payload_sha256,
            "feed_timestamp": _text(self.feed_timestamp),
            "feed_age_seconds": self.feed_age_seconds,
            "entity_count": self.entity_count,
        }

    @classmethod
    def from_record(cls, record: object) -> "CycleFeedAttempt":
        r = _require_keys(record, cls.FIELDS, "feed attempt")
        return cls(
            feed_type=r["feed_type"],
            attempt_id=r["attempt_id"],
            audit_status=r["audit_status"],
            audit_ref=r["audit_ref"],
            attempt_started_at_utc=_parse_utc(
                r["attempt_started_at_utc"], "attempt_started_at_utc"
            ),
            attempt_completed_at_utc=_parse_optional_utc(
                r["attempt_completed_at_utc"], "attempt_completed_at_utc"
            ),
            outcome=None if r["outcome"] is None else Outcome(r["outcome"]),
            error_category=(
                None
                if r["error_category"] is None
                else ErrorCategory(r["error_category"])
            ),
            http_status=_optional_int(r["http_status"], "http_status"),
            payload_ref=r["payload_ref"],
            payload_sha256=r["payload_sha256"],
            feed_timestamp=_parse_optional_utc(r["feed_timestamp"], "feed_timestamp"),
            feed_age_seconds=_optional_number(
                r["feed_age_seconds"], "feed_age_seconds"
            ),
            entity_count=_optional_int(r["entity_count"], "entity_count"),
        )


@dataclass(frozen=True, slots=True)
class PollCycleRecord:
    """One immutable record per collector execution."""

    poll_cycle_id: str
    started_at_utc: datetime
    completed_at_utc: datetime
    requested_feeds: tuple[str, ...]
    execution_name: str | None
    collector_revision: str | None
    feeds: tuple[CycleFeedAttempt, ...]

    FIELDS = (
        "schema_version",
        "poll_cycle_id",
        "started_at_utc",
        "completed_at_utc",
        "requested_feeds",
        "execution_name",
        "collector_revision",
        "feeds",
    )

    def __post_init__(self) -> None:
        _require_hex(self.poll_cycle_id, _HEX32, "poll_cycle_id")
        _require_utc(self.started_at_utc)
        _require_utc(self.completed_at_utc)
        if self.completed_at_utc < self.started_at_utc:
            raise ValueError("a cycle cannot complete before it starts")
        if (
            not isinstance(self.requested_feeds, tuple)
            or not self.requested_feeds
            or len(set(self.requested_feeds)) != len(self.requested_feeds)
            or any(feed not in FEED_TYPES for feed in self.requested_feeds)
        ):
            raise ValueError("requested_feeds must be distinct known feeds")
        if self.execution_name is not None and (
            not isinstance(self.execution_name, str) or not self.execution_name
        ):
            raise ValueError("execution_name cannot be empty")
        if self.collector_revision is not None:
            _require_hex(self.collector_revision, _HEX40, "collector_revision")
        if not isinstance(self.feeds, tuple) or not all(
            isinstance(feed, CycleFeedAttempt) for feed in self.feeds
        ):
            raise TypeError("feeds must be a tuple of CycleFeedAttempt")

        # Rule: feeds match requested_feeds one to one, in order.
        if tuple(feed.feed_type for feed in self.feeds) != self.requested_feeds:
            raise ValueError("feeds must match requested_feeds in order")
        if len({feed.attempt_id for feed in self.feeds}) != len(self.feeds):
            raise ValueError("each feed attempt needs its own attempt_id")

    @property
    def object_name(self) -> str:
        """Create-only name in the Landing bucket; sorts by completion time."""
        done = self.completed_at_utc
        return (
            f"{CYCLE_PREFIX}dt={done:%Y-%m-%d}/"
            f"{done:%Y%m%dT%H%M%S}Z_{self.poll_cycle_id}.json"
        )

    def to_record(self) -> dict[str, Any]:
        return {
            "schema_version": SCHEMA_VERSION,
            "poll_cycle_id": self.poll_cycle_id,
            "started_at_utc": _utc_text(self.started_at_utc),
            "completed_at_utc": _utc_text(self.completed_at_utc),
            "requested_feeds": list(self.requested_feeds),
            "execution_name": self.execution_name,
            "collector_revision": self.collector_revision,
            "feeds": [feed.to_record() for feed in self.feeds],
        }

    def to_json(self) -> bytes:
        return _to_json(self.to_record())

    @classmethod
    def from_record(cls, record: object) -> "PollCycleRecord":
        r = _require_keys(record, cls.FIELDS, "poll cycle record")
        if r["schema_version"] != SCHEMA_VERSION:
            raise ValueError("unknown poll cycle schema_version")
        if not isinstance(r["requested_feeds"], list) or not isinstance(
            r["feeds"], list
        ):
            raise TypeError("requested_feeds and feeds must be lists")
        return cls(
            poll_cycle_id=r["poll_cycle_id"],
            started_at_utc=_parse_utc(r["started_at_utc"], "started_at_utc"),
            completed_at_utc=_parse_utc(r["completed_at_utc"], "completed_at_utc"),
            requested_feeds=tuple(r["requested_feeds"]),
            execution_name=r["execution_name"],
            collector_revision=r["collector_revision"],
            feeds=tuple(CycleFeedAttempt.from_record(feed) for feed in r["feeds"]),
        )


# Latest status.


@dataclass(frozen=True, slots=True)
class LastAttempt:
    """Public subset of a cycle feed attempt, plus the cycle it came from."""

    poll_cycle_id: str
    cycle_completed_at_utc: datetime
    attempt_id: str
    audit_status: str
    attempt_completed_at_utc: datetime | None
    outcome: Outcome | None
    error_category: ErrorCategory | None
    http_status: int | None
    feed_timestamp: datetime | None
    feed_age_seconds: float | None
    entity_count: int | None

    FIELDS = (
        "poll_cycle_id",
        "cycle_completed_at_utc",
        "attempt_id",
        "audit_status",
        "attempt_completed_at_utc",
        "outcome",
        "error_category",
        "http_status",
        "feed_timestamp",
        "feed_age_seconds",
        "entity_count",
    )

    def __post_init__(self) -> None:
        _require_hex(self.poll_cycle_id, _HEX32, "poll_cycle_id")
        _require_hex(self.attempt_id, _HEX32, "attempt_id")
        _require_utc(self.cycle_completed_at_utc)
        _check_capture_fields(
            audit_status=self.audit_status,
            attempt_completed_at_utc=self.attempt_completed_at_utc,
            outcome=self.outcome,
            error_category=self.error_category,
            http_status=self.http_status,
            feed_timestamp=self.feed_timestamp,
            feed_age_seconds=self.feed_age_seconds,
            entity_count=self.entity_count,
            success_fields=(
                self.feed_timestamp,
                self.feed_age_seconds,
                self.entity_count,
            ),
        )

    @property
    def succeeded(self) -> bool:
        """Success needs an audit behind it, not only a successful capture."""
        return self.audit_status == AUDIT_WRITTEN and self.outcome is Outcome.SUCCESS

    @classmethod
    def from_cycle(
        cls, cycle: PollCycleRecord, attempt: CycleFeedAttempt
    ) -> "LastAttempt":
        return cls(
            poll_cycle_id=cycle.poll_cycle_id,
            cycle_completed_at_utc=cycle.completed_at_utc,
            attempt_id=attempt.attempt_id,
            audit_status=attempt.audit_status,
            attempt_completed_at_utc=attempt.attempt_completed_at_utc,
            outcome=attempt.outcome,
            error_category=attempt.error_category,
            http_status=attempt.http_status,
            feed_timestamp=attempt.feed_timestamp,
            feed_age_seconds=attempt.feed_age_seconds,
            entity_count=attempt.entity_count,
        )

    def to_record(self) -> dict[str, Any]:
        return {
            "poll_cycle_id": self.poll_cycle_id,
            "cycle_completed_at_utc": _utc_text(self.cycle_completed_at_utc),
            "attempt_id": self.attempt_id,
            "audit_status": self.audit_status,
            "attempt_completed_at_utc": _text(self.attempt_completed_at_utc),
            "outcome": None if self.outcome is None else self.outcome.value,
            "error_category": (
                None if self.error_category is None else self.error_category.value
            ),
            "http_status": self.http_status,
            "feed_timestamp": _text(self.feed_timestamp),
            "feed_age_seconds": self.feed_age_seconds,
            "entity_count": self.entity_count,
        }

    @classmethod
    def from_record(cls, record: object) -> "LastAttempt":
        r = _require_keys(record, cls.FIELDS, "last_attempt")
        return cls(
            poll_cycle_id=r["poll_cycle_id"],
            cycle_completed_at_utc=_parse_utc(
                r["cycle_completed_at_utc"], "cycle_completed_at_utc"
            ),
            attempt_id=r["attempt_id"],
            audit_status=r["audit_status"],
            attempt_completed_at_utc=_parse_optional_utc(
                r["attempt_completed_at_utc"], "attempt_completed_at_utc"
            ),
            outcome=None if r["outcome"] is None else Outcome(r["outcome"]),
            error_category=(
                None
                if r["error_category"] is None
                else ErrorCategory(r["error_category"])
            ),
            http_status=_optional_int(r["http_status"], "http_status"),
            feed_timestamp=_parse_optional_utc(r["feed_timestamp"], "feed_timestamp"),
            feed_age_seconds=_optional_number(
                r["feed_age_seconds"], "feed_age_seconds"
            ),
            entity_count=_optional_int(r["entity_count"], "entity_count"),
        )


@dataclass(frozen=True, slots=True)
class FeedStatus:
    """One feed in latest.json, with values carried over between cycles."""

    last_attempt: LastAttempt | None
    last_success_at_utc: datetime | None
    last_success_feed_timestamp: datetime | None
    last_audit_failure_at_utc: datetime | None
    last_audit_failure_poll_cycle_id: str | None

    FIELDS = (
        "last_attempt",
        "last_success_at_utc",
        "last_success_feed_timestamp",
        "last_audit_failure_at_utc",
        "last_audit_failure_poll_cycle_id",
    )

    def __post_init__(self) -> None:
        if self.last_attempt is not None and not isinstance(
            self.last_attempt, LastAttempt
        ):
            raise TypeError("last_attempt must be a LastAttempt")
        _require_optional_utc(self.last_success_at_utc)
        _require_optional_utc(self.last_success_feed_timestamp)
        _require_optional_utc(self.last_audit_failure_at_utc)
        if self.last_audit_failure_poll_cycle_id is not None:
            _require_hex(
                self.last_audit_failure_poll_cycle_id,
                _HEX32,
                "last_audit_failure_poll_cycle_id",
            )

        if (self.last_success_at_utc is None) != (
            self.last_success_feed_timestamp is None
        ):
            raise ValueError("a last success has both its times or neither")
        if (self.last_audit_failure_at_utc is None) != (
            self.last_audit_failure_poll_cycle_id is None
        ):
            raise ValueError("a last audit failure has its time and its cycle")

        last = self.last_attempt
        if last is None:
            if (
                self.last_success_at_utc is not None
                or self.last_audit_failure_at_utc is not None
            ):
                raise ValueError("a feed never attempted has no history")
            return
        # The latest attempt is reflected in the carried values.
        if last.succeeded and (
            self.last_success_at_utc != last.attempt_completed_at_utc
            or self.last_success_feed_timestamp != last.feed_timestamp
        ):
            raise ValueError("a successful last attempt is the last success")
        if last.audit_status == AUDIT_FAILED and (
            self.last_audit_failure_poll_cycle_id != last.poll_cycle_id
            or self.last_audit_failure_at_utc != last.cycle_completed_at_utc
        ):
            raise ValueError("a failed audit in the last attempt is the last failure")

    @classmethod
    def never_attempted(cls) -> "FeedStatus":
        return cls(None, None, None, None, None)

    def after(self, cycle: PollCycleRecord, attempt: CycleFeedAttempt) -> "FeedStatus":
        """Roll this feed forward by one attempt in a newer cycle."""
        last = LastAttempt.from_cycle(cycle, attempt)
        success_at, success_feed_timestamp = (
            (last.attempt_completed_at_utc, last.feed_timestamp)
            if last.succeeded
            else (self.last_success_at_utc, self.last_success_feed_timestamp)
        )
        failure_at, failure_cycle = (
            (cycle.completed_at_utc, cycle.poll_cycle_id)
            if attempt.audit_status == AUDIT_FAILED
            else (self.last_audit_failure_at_utc, self.last_audit_failure_poll_cycle_id)
        )
        return FeedStatus(
            last_attempt=last,
            last_success_at_utc=success_at,
            last_success_feed_timestamp=success_feed_timestamp,
            last_audit_failure_at_utc=failure_at,
            last_audit_failure_poll_cycle_id=failure_cycle,
        )

    def to_record(self) -> dict[str, Any]:
        return {
            "last_attempt": (
                None if self.last_attempt is None else self.last_attempt.to_record()
            ),
            "last_success_at_utc": _text(self.last_success_at_utc),
            "last_success_feed_timestamp": _text(self.last_success_feed_timestamp),
            "last_audit_failure_at_utc": _text(self.last_audit_failure_at_utc),
            "last_audit_failure_poll_cycle_id": self.last_audit_failure_poll_cycle_id,
        }

    @classmethod
    def from_record(cls, record: object) -> "FeedStatus":
        r = _require_keys(record, cls.FIELDS, "feed status")
        return cls(
            last_attempt=(
                None
                if r["last_attempt"] is None
                else LastAttempt.from_record(r["last_attempt"])
            ),
            last_success_at_utc=_parse_optional_utc(
                r["last_success_at_utc"], "last_success_at_utc"
            ),
            last_success_feed_timestamp=_parse_optional_utc(
                r["last_success_feed_timestamp"], "last_success_feed_timestamp"
            ),
            last_audit_failure_at_utc=_parse_optional_utc(
                r["last_audit_failure_at_utc"], "last_audit_failure_at_utc"
            ),
            last_audit_failure_poll_cycle_id=r["last_audit_failure_poll_cycle_id"],
        )


@dataclass(frozen=True, slots=True)
class LatestStatus:
    """The current Landing status. Always derivable from poll cycle records."""

    generated_at_utc: datetime
    stale_after_utc: datetime
    poll_cycle_id: str
    cycle_ref: str | None
    feeds: Mapping[str, FeedStatus]

    FIELDS = (
        "schema_version",
        "generated_at_utc",
        "stale_after_utc",
        "poll_cycle_id",
        "cycle_ref",
        "feeds",
    )

    def __post_init__(self) -> None:
        _require_utc(self.generated_at_utc)
        _require_utc(self.stale_after_utc)
        _require_hex(self.poll_cycle_id, _HEX32, "poll_cycle_id")
        if not isinstance(self.feeds, Mapping) or set(self.feeds) != set(FEED_TYPES):
            raise ValueError("feeds must hold exactly the three feeds")
        if not all(isinstance(status, FeedStatus) for status in self.feeds.values()):
            raise TypeError("feeds values must be FeedStatus")
        # Read-only from here on, like the rest of the frozen object.
        object.__setattr__(
            self,
            "feeds",
            MappingProxyType({feed: self.feeds[feed] for feed in FEED_TYPES}),
        )

        # Rule: stale_after_utc is later than generated_at_utc. derive() writes
        # exactly STALE_AFTER; a previous file may carry an earlier setting.
        if self.stale_after_utc <= self.generated_at_utc:
            raise ValueError("stale_after_utc must be later than generated_at_utc")

        # Rule: cycle_ref names the record of this very cycle.
        if self.cycle_ref is not None:
            done = self.generated_at_utc
            expected = (
                f"{CYCLE_PREFIX}dt={done:%Y-%m-%d}/"
                f"{done:%Y%m%dT%H%M%S}Z_{self.poll_cycle_id}.json"
            )
            if self.cycle_ref != expected:
                raise ValueError("cycle_ref must name this cycle's record")

        # Feeds attempted in this cycle carry this cycle's id and time.
        for status in self.feeds.values():
            last = status.last_attempt
            if last is not None and last.cycle_completed_at_utc > self.generated_at_utc:
                raise ValueError("a feed cannot be newer than the file")
            if (
                last is not None
                and last.poll_cycle_id == self.poll_cycle_id
                and last.cycle_completed_at_utc != self.generated_at_utc
            ):
                raise ValueError("this cycle's attempts carry its completion time")

    @classmethod
    def derive(
        cls,
        previous: "LatestStatus | None",
        cycle: PollCycleRecord,
        cycle_ref: str | None,
    ) -> "LatestStatus | None":
        """The status after `cycle`, or None when `cycle` is not newer.

        An older or equal cycle must not overwrite a newer file: two executions
        started almost together can finish in the opposite order.
        """
        if previous is not None and previous.generated_at_utc >= cycle.completed_at_utc:
            return None
        feeds = (
            dict(previous.feeds)
            if previous is not None
            else {feed: FeedStatus.never_attempted() for feed in FEED_TYPES}
        )
        for attempt in cycle.feeds:
            feeds[attempt.feed_type] = feeds[attempt.feed_type].after(cycle, attempt)
        return cls(
            generated_at_utc=cycle.completed_at_utc,
            stale_after_utc=cycle.completed_at_utc + STALE_AFTER,
            poll_cycle_id=cycle.poll_cycle_id,
            cycle_ref=cycle_ref,
            feeds=feeds,
        )

    def to_record(self) -> dict[str, Any]:
        return {
            "schema_version": SCHEMA_VERSION,
            "generated_at_utc": _utc_text(self.generated_at_utc),
            "stale_after_utc": _utc_text(self.stale_after_utc),
            "poll_cycle_id": self.poll_cycle_id,
            "cycle_ref": self.cycle_ref,
            "feeds": {feed: self.feeds[feed].to_record() for feed in FEED_TYPES},
        }

    def to_json(self) -> bytes:
        return _to_json(self.to_record())

    @classmethod
    def from_record(cls, record: object) -> "LatestStatus":
        r = _require_keys(record, cls.FIELDS, "latest status")
        if r["schema_version"] != SCHEMA_VERSION:
            raise ValueError("unknown latest status schema_version")
        feeds = _require_keys(r["feeds"], FEED_TYPES, "latest status feeds")
        return cls(
            generated_at_utc=_parse_utc(r["generated_at_utc"], "generated_at_utc"),
            stale_after_utc=_parse_utc(r["stale_after_utc"], "stale_after_utc"),
            poll_cycle_id=r["poll_cycle_id"],
            cycle_ref=r["cycle_ref"],
            feeds={feed: FeedStatus.from_record(feeds[feed]) for feed in FEED_TYPES},
        )

    @classmethod
    def from_json(cls, contents: bytes) -> "LatestStatus":
        try:
            record = json.loads(contents)
        except (UnicodeDecodeError, ValueError):
            raise ValueError("latest status is not valid JSON") from None
        return cls.from_record(record)
