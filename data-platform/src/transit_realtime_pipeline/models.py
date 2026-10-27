"""The audit model for one attempt to capture a GTFS-Realtime feed."""

import math
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum
from pathlib import PurePosixPath
from string import hexdigits


class Outcome(StrEnum):
    SUCCESS = "success"
    CONFIG_ERROR = "config_error"
    HTTP_ERROR = "http_error"
    TIMEOUT = "timeout"
    INVALID_PROTOBUF = "invalid_protobuf"
    FEED_METADATA_ERROR = "feed_metadata_error"
    FEED_CONTENT_ERROR = "feed_content_error"
    TRANSPORT_ERROR = "transport_error"
    STORAGE_ERROR = "storage_error"
    OTHER_ERROR = "other_error"


class ErrorCategory(StrEnum):
    CONFIGURATION = "configuration"
    AUTHENTICATION = "authentication"
    HTTP_CLIENT = "http_client"
    HTTP_SERVER = "http_server"
    TIMEOUT = "timeout"
    INVALID_PROTOBUF = "invalid_protobuf"
    FEED_METADATA = "feed_metadata"
    FEED_CONTENT = "feed_content"
    TRANSPORT = "transport"
    STORAGE = "storage"
    OTHER = "other"


def _require_utc(value: datetime) -> None:
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError("timestamps must be timezone-aware UTC")


def _utc_text(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


@dataclass(frozen=True, slots=True)
class CaptureAttempt:
    """One capture invocation and the content it obtained, if any.

    An invocation may fail before an HTTP request is sent. attempt_id identifies
    that invocation; payload_sha256 identifies content. Multiple invocations
    may therefore refer to the same payload_ref.
    """

    attempt_id: str
    source_id: str
    feed_type: str
    started_at_utc: datetime
    completed_at_utc: datetime
    outcome: Outcome

    poll_cycle_id: str | None = None
    http_status: int | None = None
    payload_ref: str | None = None
    payload_sha256: str | None = None
    payload_bytes: int | None = None

    feed_timestamp: datetime | None = None
    feed_age_seconds: float | None = None
    incrementality: str | None = None
    entity_count: int | None = None
    vehicle_count: int | None = None
    trip_update_count: int | None = None
    alert_count: int | None = None

    error_category: ErrorCategory | None = None

    def __post_init__(self) -> None:
        if not self.attempt_id or not self.source_id or not self.feed_type:
            raise ValueError("attempt_id, source_id and feed_type are required")

        if self.poll_cycle_id is not None and not self.poll_cycle_id:
            raise ValueError("poll_cycle_id cannot be empty")

        if not isinstance(self.outcome, Outcome):
            raise TypeError("outcome must be a known category")

        if self.error_category is not None and not isinstance(
            self.error_category, ErrorCategory
        ):
            # Raw exception messages are not valid categories.
            raise ValueError("error_category must be a known category")

        _require_utc(self.started_at_utc)
        _require_utc(self.completed_at_utc)

        if self.completed_at_utc < self.started_at_utc:
            raise ValueError("completed_at_utc cannot precede started_at_utc")

        if self.feed_timestamp is not None:
            _require_utc(self.feed_timestamp)

        if self.http_status is not None and not 100 <= self.http_status <= 599:
            raise ValueError("http_status must be a valid HTTP status")

        if self.payload_ref is not None:
            path = PurePosixPath(self.payload_ref)
            if (
                not path.parts
                or path.is_absolute()
                or ".." in path.parts
                or any(char in self.payload_ref for char in (":", "\\", "?", "#"))
            ):
                raise ValueError("payload_ref must be a relative object reference")

        if self.payload_sha256 is not None:
            digest = self.payload_sha256
            if len(digest) != 64 or any(char not in hexdigits for char in digest):
                raise ValueError("payload_sha256 must be a full SHA-256 hex digest")

        if self.payload_bytes is not None and self.payload_bytes < 0:
            raise ValueError("payload_bytes cannot be negative")

        for count in (
            self.entity_count,
            self.vehicle_count,
            self.trip_update_count,
            self.alert_count,
        ):
            if count is not None and count < 0:
                raise ValueError("entity counts cannot be negative")

        if self.feed_age_seconds is not None and not math.isfinite(
            self.feed_age_seconds
        ):
            raise ValueError("feed_age_seconds must be finite")

        if self.incrementality is not None and self.incrementality not in {
            "FULL_DATASET",
            "DIFFERENTIAL",
        }:
            raise ValueError("incrementality must be a known GTFS-RT value")

        if self.outcome is Outcome.SUCCESS:
            required = (
                self.payload_ref,
                self.payload_sha256,
                self.payload_bytes,
                self.feed_timestamp,
                self.feed_age_seconds,
                self.incrementality,
                self.entity_count,
            )
            if (
                self.http_status is None
                or not 200 <= self.http_status < 300
                or any(value is None for value in required)
            ):
                raise ValueError(
                    "success requires HTTP 2xx, a saved payload "
                    "and parsed feed metadata"
                )
            if self.error_category is not None:
                raise ValueError("success cannot have an error_category")

        elif self.error_category is None:
            raise ValueError("a failed attempt requires an error_category")

        if self.outcome is Outcome.HTTP_ERROR and (
            self.http_status is None or 200 <= self.http_status < 300
        ):
            raise ValueError("http_error requires a non-2xx HTTP status")

    def to_record(self) -> dict[str, str | int | float]:
        """Emit only approved, JSON-ready fields."""
        record: dict[str, str | int | float | None] = {
            "attempt_id": self.attempt_id,
            "poll_cycle_id": self.poll_cycle_id,
            "source_id": self.source_id,
            "feed_type": self.feed_type,
            "started_at_utc": _utc_text(self.started_at_utc),
            "completed_at_utc": _utc_text(self.completed_at_utc),
            "outcome": self.outcome.value,
            "http_status": self.http_status,
            "payload_ref": self.payload_ref,
            "payload_sha256": self.payload_sha256,
            "payload_bytes": self.payload_bytes,
            "feed_timestamp": (
                _utc_text(self.feed_timestamp)
                if self.feed_timestamp is not None
                else None
            ),
            "feed_age_seconds": self.feed_age_seconds,
            "incrementality": self.incrementality,
            "entity_count": self.entity_count,
            "vehicle_count": self.vehicle_count,
            "trip_update_count": self.trip_update_count,
            "alert_count": self.alert_count,
            "error_category": (
                self.error_category.value if self.error_category is not None else None
            ),
        }
        return {name: value for name, value in record.items() if value is not None}
