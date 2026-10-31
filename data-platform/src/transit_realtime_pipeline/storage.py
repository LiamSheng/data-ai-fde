"""Persistence adapters and the interfaces consumed by the capture workflow."""

import hashlib
import json
import os
import tempfile
from pathlib import Path, PurePosixPath
from typing import Any, Protocol

from google.api_core.exceptions import NotFound, PreconditionFailed

from .config import FEED_TYPES
from .models import CaptureAttempt
from .status import CYCLE_PREFIX as POLL_CYCLE_PREFIX


class CaptureStorage(Protocol):
    """The persistence operations needed by capture_one.

    The capture workflow depends on these operations, not on the GCS SDK.
    """

    def save_raw_payload(
        self, payload: bytes, feed_type: str
    ) -> tuple[str, str, str]: ...

    def write_attempt_record(self, attempt: CaptureAttempt) -> str: ...

    def write_poll_cycle_record(self, name: str, contents: bytes) -> None: ...


class LatestStatusStore(Protocol):
    """Where latest.json lives: read the previous one, then overwrite it."""

    def read(self) -> bytes | None: ...

    def write(self, contents: bytes) -> None: ...


# Object name in the status bucket and, locally, under data/status/.
LATEST_STATUS_OBJECT = "landing/v1/latest.json"

# GCS gives public objects "public, max-age=3600" by default; latest.json would
# then be served up to an hour old. It changes every cycle, so never cache it.
LATEST_STATUS_CACHE_CONTROL = "no-store"


class ScheduleStorage(Protocol):
    """Persistence operations for static GTFS schedule archives."""

    def save_schedule_archive(self, payload: bytes) -> tuple[str, str, str]: ...

    def write_schedule_attempt_record(self, record: dict[str, object]) -> str: ...


class GCSStorage:
    """Persist content-addressed payloads and independent attempt records."""

    def __init__(self, bucket_name: str, *, client: Any | None = None) -> None:
        if (
            not bucket_name
            or bucket_name != bucket_name.strip()
            or any(character in bucket_name for character in ("/", ":", "?"))
        ):
            raise ValueError("invalid storage configuration")

        # Tests inject a fake client.
        # Only real GCS mode constructs the SDK client and uses credentials.
        if client is None:
            from google.cloud import storage

            client = storage.Client()

        # bucket() creates a reference to an existing bucket.
        # It does not create infrastructure.
        self._bucket = client.bucket(bucket_name)

    def save_raw_payload(self, payload: bytes, feed_type: str) -> tuple[str, str, str]:
        if feed_type not in FEED_TYPES:
            raise ValueError("unknown feed type")

        digest = hashlib.sha256(payload).hexdigest()
        payload_ref = PurePosixPath(feed_type, digest[:2], f"{digest}.pb").as_posix()

        try:
            self._bucket.blob(payload_ref).upload_from_string(
                payload,
                content_type="application/octet-stream",
                if_generation_match=0,
                timeout=20.0,
                retry=None,
            )
        except PreconditionFailed:
            # A live object already exists at this name.
            # We do not download it or verify its existing content.
            return payload_ref, digest, "existing"
        except Exception:  # noqa: BLE001 - redact SDK exception details
            raise OSError("payload upload failed") from None

        return payload_ref, digest, "new"

    def write_attempt_record(self, attempt: CaptureAttempt) -> str:
        record_ref = f"audit/capture_attempts/{attempt.attempt_id}.json"
        contents = (
            json.dumps(
                attempt.to_record(),
                indent=2,
                sort_keys=True,
                allow_nan=False,
            )
            + "\n"
        ).encode("utf-8")

        try:
            self._bucket.blob(record_ref).upload_from_string(
                contents,
                content_type="application/json",
                if_generation_match=0,
                timeout=20.0,
                retry=None,
            )
        except Exception:  # noqa: BLE001 - redact SDK exception details
            # An existing audit is not proof that this write succeeded.
            # Collisions and upload failures therefore remain failures.
            raise OSError("audit upload failed") from None

        return record_ref

    def write_poll_cycle_record(self, name: str, contents: bytes) -> None:
        """Create one poll cycle record; an existing object is a failure."""
        if not name.startswith(POLL_CYCLE_PREFIX):
            raise ValueError("invalid poll cycle record name")
        try:
            self._bucket.blob(name).upload_from_string(
                contents,
                content_type="application/json",
                if_generation_match=0,
                timeout=20.0,
                retry=None,
            )
        except Exception:  # noqa: BLE001 - redact SDK exception details
            raise OSError("poll cycle record upload failed") from None

    def save_schedule_archive(self, payload: bytes) -> tuple[str, str, str]:
        """Create an immutable content-addressed GTFS Schedule ZIP object."""
        digest = hashlib.sha256(payload).hexdigest()
        payload_ref = PurePosixPath(
            "gtfs_schedule", digest[:2], f"{digest}.zip"
        ).as_posix()

        try:
            self._bucket.blob(payload_ref).upload_from_string(
                payload,
                content_type="application/zip",
                if_generation_match=0,
                timeout=20.0,
                retry=None,
            )
        except PreconditionFailed:
            # Existing bytes are not downloaded or checked in cloud mode.
            return payload_ref, digest, "existing"
        except Exception:  # noqa: BLE001 - redact SDK exception details
            raise OSError("schedule archive upload failed") from None

        return payload_ref, digest, "new"

    def write_schedule_attempt_record(self, record: dict[str, object]) -> str:
        """Write one static-feed attempt to its independent audit object."""
        attempt_id = record.get("attempt_id")
        if not isinstance(attempt_id, str) or not attempt_id:
            raise ValueError("invalid schedule attempt")

        record_ref = f"audit/schedule_capture_attempts/{attempt_id}.json"
        contents = (
            json.dumps(record, indent=2, sort_keys=True, allow_nan=False) + "\n"
        ).encode("utf-8")

        try:
            self._bucket.blob(record_ref).upload_from_string(
                contents,
                content_type="application/json",
                if_generation_match=0,
                timeout=20.0,
                retry=None,
            )
        except Exception:  # noqa: BLE001 - redact SDK exception details
            raise OSError("schedule audit upload failed") from None

        return record_ref


class GCSLatestStatusStore:
    """latest.json in the status bucket, separate from the Landing bucket.

    Overwriting is allowed here and only here: the file is derived and can be
    rebuilt from poll cycle records.
    """

    def __init__(self, bucket_name: str, *, client: Any | None = None) -> None:
        if (
            not bucket_name
            or bucket_name != bucket_name.strip()
            or any(character in bucket_name for character in ("/", ":", "?"))
        ):
            raise ValueError("invalid status storage configuration")
        if client is None:
            from google.cloud import storage

            client = storage.Client()
        self._bucket = client.bucket(bucket_name)

    def read(self) -> bytes | None:
        try:
            return self._bucket.blob(LATEST_STATUS_OBJECT).download_as_bytes(
                timeout=20.0, retry=None
            )
        except NotFound:
            return None
        except Exception:  # noqa: BLE001 - redact SDK exception details
            raise OSError("latest status download failed") from None

    def write(self, contents: bytes) -> None:
        blob = self._bucket.blob(LATEST_STATUS_OBJECT)
        blob.cache_control = LATEST_STATUS_CACHE_CONTROL
        try:
            blob.upload_from_string(
                contents,
                content_type="application/json",
                timeout=20.0,
                retry=None,
            )
        except Exception:  # noqa: BLE001 - redact SDK exception details
            raise OSError("latest status upload failed") from None


def write_local_poll_cycle_record(root: Path, name: str, contents: bytes) -> Path:
    """Create one poll cycle record under data/ops/ without replacing any file.

    Local mode mirrors the Landing bucket with audit/ mapped to data/ops/.
    """
    if not name.startswith(POLL_CYCLE_PREFIX):
        raise ValueError("invalid poll cycle record name")
    relative = PurePosixPath(name).relative_to("audit")
    destination = root.joinpath("data", "ops", *relative.parts)
    destination.parent.mkdir(parents=True, exist_ok=True)

    descriptor, temporary_name = tempfile.mkstemp(
        prefix=".pending-", dir=destination.parent
    )
    try:
        with os.fdopen(descriptor, "wb") as temporary_file:
            temporary_file.write(contents)
            temporary_file.flush()
            os.fsync(temporary_file.fileno())
        # Linking creates the destination atomically and never overwrites it.
        os.link(temporary_name, destination)
    finally:
        Path(temporary_name).unlink(missing_ok=True)
    return destination


class LocalLatestStatusStore:
    """latest.json under data/status/, replaced atomically."""

    def __init__(self, root: Path) -> None:
        self.path = root.joinpath("data", "status", *LATEST_STATUS_OBJECT.split("/"))

    def read(self) -> bytes | None:
        try:
            return self.path.read_bytes()
        except FileNotFoundError:
            return None

    def write(self, contents: bytes) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=".pending-", dir=self.path.parent
        )
        try:
            with os.fdopen(descriptor, "wb") as temporary_file:
                temporary_file.write(contents)
                temporary_file.flush()
                os.fsync(temporary_file.fileno())
            os.replace(temporary_name, self.path)
        finally:
            Path(temporary_name).unlink(missing_ok=True)


def configured_latest_status_store() -> LatestStatusStore | None:
    """Select where latest.json goes for this cycle.

    None means GCS mode without GTFS_RT_STATUS_BUCKET: the status is skipped,
    never redirected to another bucket or to local files. Capture still runs.
    """
    backend = os.getenv("GTFS_RT_STORAGE_BACKEND", "local")

    if backend == "local":
        return LocalLatestStatusStore(Path.cwd())

    if backend != "gcs":
        raise ValueError("invalid storage configuration")

    bucket_name = os.getenv("GTFS_RT_STATUS_BUCKET")
    if not bucket_name:
        return None

    return GCSLatestStatusStore(bucket_name)


def configured_cloud_storage() -> CaptureStorage | None:
    """Select local mode or construct GCS persistence for one poll cycle.

    None means: retain the existing local persistence functions.
    """

    backend = os.getenv("GTFS_RT_STORAGE_BACKEND", "local")

    if backend == "local":
        return None

    if backend != "gcs":
        raise ValueError("invalid storage configuration")

    bucket_name = os.getenv("GTFS_RT_GCS_BUCKET")
    if not bucket_name:
        raise ValueError("invalid storage configuration")

    return GCSStorage(bucket_name)
