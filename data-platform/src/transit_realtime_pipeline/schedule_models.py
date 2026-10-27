"""Audit contract for one static GTFS Schedule download attempt."""

from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum


class ScheduleOutcome(StrEnum):
    SUCCESS = "success"
    CONFIG_ERROR = "config_error"
    HTTP_ERROR = "http_error"
    TIMEOUT = "timeout"
    TRANSPORT_ERROR = "transport_error"
    INVALID_ARCHIVE = "invalid_archive"
    STORAGE_ERROR = "storage_error"
    OTHER_ERROR = "other_error"


class ScheduleErrorCategory(StrEnum):
    CONFIGURATION = "configuration"
    AUTHENTICATION = "authentication"
    HTTP_CLIENT = "http_client"
    HTTP_SERVER = "http_server"
    TIMEOUT = "timeout"
    TRANSPORT = "transport"
    INVALID_ARCHIVE = "invalid_archive"
    STORAGE = "storage"
    OTHER = "other"


def _utc_text(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


@dataclass(frozen=True, slots=True)
class ScheduleAttempt:
    """One request for the current static schedule file, successful or not."""

    attempt_id: str
    started_at_utc: datetime
    completed_at_utc: datetime
    outcome: ScheduleOutcome
    http_status: int | None = None
    error_category: ScheduleErrorCategory | None = None
    payload_ref: str | None = None
    payload_sha256: str | None = None
    payload_bytes: int | None = None
    storage_disposition: str | None = None
    archive_files: tuple[str, ...] = ()
    feed_version: str | None = None
    feed_start_date: str | None = None
    feed_end_date: str | None = None
    calendar_start_date: str | None = None
    calendar_end_date: str | None = None
    calendar_exception_add_start_date: str | None = None
    calendar_exception_add_end_date: str | None = None

    def __post_init__(self) -> None:
        if not self.attempt_id:
            raise ValueError("attempt_id is required")
        for value in (self.started_at_utc, self.completed_at_utc):
            if value.tzinfo is None or value.utcoffset() != timedelta(0):
                raise ValueError("timestamps must be timezone-aware UTC")
        if self.completed_at_utc < self.started_at_utc:
            raise ValueError("completed_at_utc cannot precede started_at_utc")
        if self.http_status is not None and not 100 <= self.http_status <= 599:
            raise ValueError("http_status must be a valid HTTP status")
        if self.payload_bytes is not None and self.payload_bytes < 0:
            raise ValueError("payload_bytes cannot be negative")
        if self.outcome is ScheduleOutcome.SUCCESS:
            if (
                self.http_status is None
                or not 200 <= self.http_status < 300
                or self.payload_ref is None
                or self.payload_sha256 is None
                or self.payload_bytes is None
                or self.error_category is not None
            ):
                raise ValueError("success requires HTTP and persisted archive metadata")
        elif self.error_category is None:
            raise ValueError("failed attempts require an error category")

    def to_record(self) -> dict[str, object]:
        """Return JSON-ready allowlisted fields; never serialize request URLs."""
        record: dict[str, object | None] = {
            "attempt_id": self.attempt_id,
            "source_id": "primary",
            "feed_type": "gtfs_schedule",
            "started_at_utc": _utc_text(self.started_at_utc),
            "completed_at_utc": _utc_text(self.completed_at_utc),
            "outcome": self.outcome.value,
            "http_status": self.http_status,
            "error_category": (
                self.error_category.value if self.error_category is not None else None
            ),
            "payload_ref": self.payload_ref,
            "payload_sha256": self.payload_sha256,
            "payload_bytes": self.payload_bytes,
            "storage_disposition": self.storage_disposition,
            "archive_files": list(self.archive_files) if self.archive_files else None,
            "feed_version": self.feed_version,
            "feed_start_date": self.feed_start_date,
            "feed_end_date": self.feed_end_date,
            "calendar_start_date": self.calendar_start_date,
            "calendar_end_date": self.calendar_end_date,
            "calendar_exception_add_start_date": self.calendar_exception_add_start_date,
            "calendar_exception_add_end_date": self.calendar_exception_add_end_date,
        }
        return {name: value for name, value in record.items() if value is not None}
