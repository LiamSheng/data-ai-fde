"""GCS persistence and the interface consumed by the capture workflow."""

import hashlib
import json
import os
from pathlib import PurePosixPath
from typing import Any, Protocol

from google.api_core.exceptions import PreconditionFailed

from .config import FEED_TYPES
from .models import CaptureAttempt


class CaptureStorage(Protocol):
    """The persistence operations needed by capture_one.

    The capture workflow depends on these operations, not on the GCS SDK.
    """

    def save_raw_payload(
        self, payload: bytes, feed_type: str
    ) -> tuple[str, str, str]: ...

    def write_attempt_record(self, attempt: CaptureAttempt) -> str: ...


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
