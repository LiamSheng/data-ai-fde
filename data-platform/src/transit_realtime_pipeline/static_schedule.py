"""Capture and inspect one downloadable GTFS Schedule archive."""

import csv
import hashlib
import io
import json
import os
import sys
import tempfile
import zipfile
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from urllib.parse import urlsplit
from uuid import uuid4

import httpx

from .schedule_models import (
    ScheduleAttempt,
    ScheduleErrorCategory,
    ScheduleOutcome,
)
from .storage import ScheduleStorage, configured_cloud_storage

SCHEDULE_URL_ENV = "GTFS_SCHEDULE_URL"


class InvalidScheduleArchive(ValueError):
    """The response is not a readable archive with a minimum GTFS file set."""


@dataclass(frozen=True, slots=True)
class ScheduleArchiveInfo:
    files: tuple[str, ...]
    feed_version: str | None = None
    feed_start_date: str | None = None
    feed_end_date: str | None = None
    calendar_start_date: str | None = None
    calendar_end_date: str | None = None
    calendar_exception_add_start_date: str | None = None
    calendar_exception_add_end_date: str | None = None


def _validate_schedule_url(url: str) -> str:
    if not url or url != url.strip():
        raise ValueError("schedule download URL is missing or invalid")
    parts = urlsplit(url)
    if (
        parts.scheme != "https"
        or not parts.hostname
        or parts.username is not None
        or parts.password is not None
        or parts.query
        or parts.fragment
        or any(character.isspace() for character in url)
    ):
        raise ValueError("schedule download URL must be a keyless HTTPS URL")
    return url


def load_schedule_url() -> str:
    """Read a public HTTPS download URL without echoing it in errors or audits."""
    return _validate_schedule_url(os.getenv(SCHEDULE_URL_ENV, ""))


def _read_csv_rows(archive: zipfile.ZipFile, name: str) -> list[dict[str, str]]:
    with archive.open(name) as raw_file:
        text_file = io.TextIOWrapper(raw_file, encoding="utf-8-sig", newline="")
        return list(csv.DictReader(text_file))


def _ordered_date_range(values: list[str | None]) -> tuple[str | None, str | None]:
    present = [value for value in values if value]
    # GTFS dates use YYYYMMDD, whose lexical and chronological order match.
    # Full value validation belongs to a schedule validator, not ZIP capture.
    if not present:
        return None, None
    return min(present), max(present)


def inspect_schedule_archive(payload: bytes) -> ScheduleArchiveInfo:
    """Check ZIP integrity and required table presence; do not extract files."""
    try:
        with zipfile.ZipFile(io.BytesIO(payload)) as archive:
            members = [
                item.filename for item in archive.infolist() if not item.is_dir()
            ]
            if not members:
                raise InvalidScheduleArchive("archive contains no files")
            if len(members) != len(set(members)):
                raise InvalidScheduleArchive("archive contains duplicate filenames")
            corrupt_member = archive.testzip()
            if corrupt_member is not None:
                raise InvalidScheduleArchive("archive member failed CRC validation")

            # GTFS Schedule tables are at the archive root. No paths are extracted.
            root_files = {
                PurePosixPath(name).name
                for name in members
                if PurePosixPath(name).parent == PurePosixPath(".")
            }
            required = {"agency.txt", "routes.txt", "trips.txt", "stop_times.txt"}
            missing = required - root_files
            if missing:
                raise InvalidScheduleArchive("archive is missing required GTFS tables")
            if "stops.txt" not in root_files and "locations.geojson" not in root_files:
                raise InvalidScheduleArchive("archive is missing stop/location data")
            if not {"calendar.txt", "calendar_dates.txt"} & root_files:
                raise InvalidScheduleArchive("archive is missing service calendar data")

            info: dict[str, str | None] = {}
            if "feed_info.txt" in root_files:
                rows = _read_csv_rows(archive, "feed_info.txt")
                first = rows[0] if rows else {}
                info["feed_version"] = first.get("feed_version") or None
                info["feed_start_date"] = first.get("feed_start_date") or None
                info["feed_end_date"] = first.get("feed_end_date") or None

            calendar_rows = (
                _read_csv_rows(archive, "calendar.txt")
                if "calendar.txt" in root_files
                else []
            )
            start_date, _ = _ordered_date_range(
                [row.get("start_date") for row in calendar_rows]
            )
            _, end_date = _ordered_date_range(
                [row.get("end_date") for row in calendar_rows]
            )

            exception_rows = (
                _read_csv_rows(archive, "calendar_dates.txt")
                if "calendar_dates.txt" in root_files
                else []
            )
            add_start, add_end = _ordered_date_range(
                [
                    row.get("date")
                    for row in exception_rows
                    if row.get("exception_type") == "1"
                ]
            )

            return ScheduleArchiveInfo(
                files=tuple(sorted(members)),
                feed_version=info.get("feed_version"),
                feed_start_date=info.get("feed_start_date"),
                feed_end_date=info.get("feed_end_date"),
                calendar_start_date=start_date,
                calendar_end_date=end_date,
                calendar_exception_add_start_date=add_start,
                calendar_exception_add_end_date=add_end,
            )
    except InvalidScheduleArchive:
        raise
    except (
        zipfile.BadZipFile,
        zipfile.LargeZipFile,
        RuntimeError,
        OSError,
        EOFError,
        NotImplementedError,
        UnicodeDecodeError,
        csv.Error,
    ):
        raise InvalidScheduleArchive(
            "response is not a readable GTFS archive"
        ) from None


def save_local_schedule_archive(
    payload: bytes, storage_root: Path
) -> tuple[str, str, str]:
    """Persist a local ZIP once, checking existing bytes before reuse."""
    digest = hashlib.sha256(payload).hexdigest()
    payload_ref = PurePosixPath("gtfs_schedule", digest[:2], f"{digest}.zip")
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
        try:
            os.link(temporary_name, destination)
            disposition = "new"
        except FileExistsError:
            disposition = "existing"
        if destination.read_bytes() != payload:
            raise OSError("stored schedule archive does not match response")
    finally:
        Path(temporary_name).unlink(missing_ok=True)
    return payload_ref.as_posix(), digest, disposition


def write_local_schedule_attempt(record: dict[str, object], record_root: Path) -> Path:
    """Atomically create one allowlisted schedule audit record."""
    record_root.mkdir(parents=True, exist_ok=True)
    destination = record_root / f"{record['attempt_id']}.json"
    contents = (
        json.dumps(record, indent=2, sort_keys=True, allow_nan=False) + "\n"
    ).encode("utf-8")
    descriptor, temporary_name = tempfile.mkstemp(prefix=".pending-", dir=record_root)
    try:
        with os.fdopen(descriptor, "wb") as temporary_file:
            temporary_file.write(contents)
            temporary_file.flush()
            os.fsync(temporary_file.fileno())
        os.link(temporary_name, destination)
    finally:
        Path(temporary_name).unlink(missing_ok=True)
    return destination


def _persist_attempt(
    attempt: ScheduleAttempt,
    *,
    record_root: Path,
    storage: ScheduleStorage | None,
) -> str:
    if storage is None:
        path = write_local_schedule_attempt(attempt.to_record(), record_root)
        return path.relative_to(Path.cwd()).as_posix()
    return storage.write_schedule_attempt_record(attempt.to_record())


def capture_schedule_once(
    *,
    storage: ScheduleStorage | None = None,
    url: str | None = None,
) -> int:
    """Fetch one ZIP and persist the bytes plus a separate attempt record."""
    attempt_id = uuid4().hex
    started_at = datetime.now(UTC)
    storage_root = Path.cwd() / "data/raw"
    record_root = Path.cwd() / "data/audit/schedule_capture_attempts"
    status: int | None = None
    payload_ref: str | None = None
    payload_sha256: str | None = None
    payload_bytes: int | None = None
    storage_disposition: str | None = None
    archive_info: ScheduleArchiveInfo | None = None
    outcome: ScheduleOutcome
    error_category: ScheduleErrorCategory | None = None

    try:
        request_url = (
            _validate_schedule_url(url) if url is not None else load_schedule_url()
        )
    except ValueError:
        outcome = ScheduleOutcome.CONFIG_ERROR
        error_category = ScheduleErrorCategory.CONFIGURATION
    else:
        try:
            with httpx.Client(
                timeout=60.0, follow_redirects=False, trust_env=False
            ) as client:
                response = client.get(request_url)
        except httpx.TimeoutException:
            outcome = ScheduleOutcome.TIMEOUT
            error_category = ScheduleErrorCategory.TIMEOUT
        except httpx.HTTPError:
            # HTTP exceptions may contain a full URL; never print or serialize them.
            outcome = ScheduleOutcome.TRANSPORT_ERROR
            error_category = ScheduleErrorCategory.TRANSPORT
        except Exception:  # noqa: BLE001 - redact untrusted exception details
            outcome = ScheduleOutcome.OTHER_ERROR
            error_category = ScheduleErrorCategory.OTHER
        else:
            status = response.status_code
            if not 200 <= status < 300:
                outcome = ScheduleOutcome.HTTP_ERROR
                if status in (401, 403):
                    error_category = ScheduleErrorCategory.AUTHENTICATION
                elif 400 <= status < 500:
                    error_category = ScheduleErrorCategory.HTTP_CLIENT
                elif 500 <= status < 600:
                    error_category = ScheduleErrorCategory.HTTP_SERVER
                else:
                    error_category = ScheduleErrorCategory.OTHER
            else:
                payload = response.content
                payload_bytes = len(payload)
                payload_sha256 = hashlib.sha256(payload).hexdigest()
                try:
                    if storage is None:
                        payload_ref, payload_sha256, storage_disposition = (
                            save_local_schedule_archive(payload, storage_root)
                        )
                    else:
                        payload_ref, payload_sha256, storage_disposition = (
                            storage.save_schedule_archive(payload)
                        )
                except Exception:  # noqa: BLE001 - redact filesystem/SDK errors
                    outcome = ScheduleOutcome.STORAGE_ERROR
                    error_category = ScheduleErrorCategory.STORAGE
                else:
                    try:
                        archive_info = inspect_schedule_archive(payload)
                    except InvalidScheduleArchive:
                        outcome = ScheduleOutcome.INVALID_ARCHIVE
                        error_category = ScheduleErrorCategory.INVALID_ARCHIVE
                    else:
                        outcome = ScheduleOutcome.SUCCESS

    return _finish_attempt(
        attempt_id,
        started_at,
        outcome,
        error_category,
        status,
        payload_ref,
        payload_sha256,
        payload_bytes,
        storage_disposition,
        archive_info,
        record_root,
        storage,
    )


def _finish_attempt(
    attempt_id: str,
    started_at: datetime,
    outcome: ScheduleOutcome,
    error_category: ScheduleErrorCategory | None,
    http_status: int | None,
    payload_ref: str | None,
    payload_sha256: str | None,
    payload_bytes: int | None,
    storage_disposition: str | None,
    archive_info: ScheduleArchiveInfo | None,
    record_root: Path,
    storage: ScheduleStorage | None,
) -> int:
    completed_at = max(datetime.now(UTC), started_at)
    attempt = ScheduleAttempt(
        attempt_id=attempt_id,
        started_at_utc=started_at,
        completed_at_utc=completed_at,
        outcome=outcome,
        http_status=http_status,
        error_category=error_category,
        payload_ref=payload_ref,
        payload_sha256=payload_sha256,
        payload_bytes=payload_bytes,
        storage_disposition=storage_disposition,
        archive_files=archive_info.files if archive_info else (),
        feed_version=archive_info.feed_version if archive_info else None,
        feed_start_date=archive_info.feed_start_date if archive_info else None,
        feed_end_date=archive_info.feed_end_date if archive_info else None,
        calendar_start_date=archive_info.calendar_start_date if archive_info else None,
        calendar_end_date=archive_info.calendar_end_date if archive_info else None,
        calendar_exception_add_start_date=(
            archive_info.calendar_exception_add_start_date if archive_info else None
        ),
        calendar_exception_add_end_date=(
            archive_info.calendar_exception_add_end_date if archive_info else None
        ),
    )

    try:
        record_ref = _persist_attempt(
            attempt,
            record_root=record_root,
            storage=storage,
        )
    except Exception:  # noqa: BLE001 - raw errors may expose URLs or local paths
        print("audit_unavailable", file=sys.stderr)
        print(f"attempt_id={attempt_id}")
        return 3

    print(f"attempt_id={attempt_id}")
    print(f"outcome={outcome.value}")
    if http_status is not None:
        print(f"http_status={http_status}")
    if payload_ref is not None:
        print(f"payload_ref={payload_ref}")
        print(f"payload_sha256={payload_sha256}")
        print(f"payload_bytes={payload_bytes}")
        print(f"storage={storage_disposition}")
    if archive_info is not None:
        print(f"archive_file_count={len(archive_info.files)}")
        if archive_info.feed_version is not None:
            print(f"feed_version={archive_info.feed_version}")
        if archive_info.feed_start_date or archive_info.feed_end_date:
            print(
                "feed_date_range="
                f"{archive_info.feed_start_date or 'unknown'}.."
                f"{archive_info.feed_end_date or 'unknown'}"
            )
        if archive_info.calendar_start_date or archive_info.calendar_end_date:
            print(
                "calendar_base_range="
                f"{archive_info.calendar_start_date or 'unknown'}.."
                f"{archive_info.calendar_end_date or 'unknown'}"
            )
    print(f"attempt_record={record_ref}")
    return 0 if outcome is ScheduleOutcome.SUCCESS else 1


def main() -> int:
    try:
        storage = configured_cloud_storage()
    except Exception:  # noqa: BLE001 - redact credential/SDK exception details
        print("storage_initialization_error", file=sys.stderr)
        return 3
    return capture_schedule_once(storage=storage)


if __name__ == "__main__":
    raise SystemExit(main())
