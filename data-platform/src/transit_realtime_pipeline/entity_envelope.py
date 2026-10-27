"""Extract entity envelopes from one saved GTFS-Realtime payload.

An envelope joins feed-level metadata with one FeedEntity. It deliberately
does not interpret the nested vehicle fields or print identifying values.
"""

import argparse
import sys
from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from hashlib import sha256
from pathlib import Path, PurePosixPath

from google.protobuf.descriptor import FieldDescriptor
from google.transit import gtfs_realtime_pb2

from .gtfs_rt import InvalidFeed, parse_feed


@dataclass(frozen=True, slots=True)
class EntityEnvelope:
    payload_ref: str
    payload_sha256: str
    entity_index: int
    feed_version: str
    feed_timestamp_utc: datetime | None
    incrementality: str
    entity_id: str
    message_fields: tuple[str, ...]
    is_deleted: bool | None

    @property
    def row_key(self) -> tuple[str, int]:
        """Identify this entity within this exact payload."""
        return self.payload_sha256, self.entity_index


def validate_payload_ref(payload_ref: str) -> PurePosixPath:
    """Accept only a normalized, relative storage reference."""
    ref = PurePosixPath(payload_ref)

    if (
        not payload_ref
        or ref.is_absolute()
        or ".." in ref.parts
        or "\\" in payload_ref
        or ref.as_posix() != payload_ref
    ):
        raise ValueError("invalid payload reference")

    return ref


def extract_entity_envelopes(payload: bytes, payload_ref: str) -> list[EntityEnvelope]:
    """Combine the feed header with every entity in one immutable payload."""
    ref = validate_payload_ref(payload_ref)
    digest = sha256(payload).hexdigest()

    # Our capture step names raw files by their content hash. Check that
    # the reference and the bytes really belong together.
    if ref.name != f"{digest}.pb":
        raise ValueError("payload reference does not match content")

    metadata = parse_feed(payload)

    # parse_feed validates the message; this second parse gives us access
    # to each entity. The saved .pb remains the original source of truth.
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(payload)

    return [
        EntityEnvelope(
            payload_ref=ref.as_posix(),
            payload_sha256=digest,
            entity_index=index,
            feed_version=metadata.version,
            feed_timestamp_utc=metadata.feed_timestamp,
            incrementality=metadata.incrementality,
            entity_id=entity.id,
            # These are the nested message types actually present, such as
            # "vehicle". We do not assume every future feed is identical.
            message_fields=tuple(
                field.name
                for field, _ in entity.ListFields()
                if field.type == FieldDescriptor.TYPE_MESSAGE
            ),
            # Missing and explicitly false are different in Protobuf.
            is_deleted=(entity.is_deleted if entity.HasField("is_deleted") else None),
        )
        for index, entity in enumerate(feed.entity)
    ]


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Inspect entity envelopes in one saved GTFS-RT payload"
    )
    parser.add_argument(
        "payload_ref",
        help="Relative reference beneath the storage root",
    )
    parser.add_argument(
        "--storage-root",
        type=Path,
        default=Path("data/raw/gtfs_rt"),
    )
    args = parser.parse_args()

    try:
        ref = validate_payload_ref(args.payload_ref)
    except ValueError:
        print("invalid_payload_ref", file=sys.stderr)
        return 1

    try:
        payload = args.storage_root.joinpath(*ref.parts).read_bytes()
    except OSError:
        print("storage_error: cannot read payload", file=sys.stderr)
        return 1

    try:
        rows = extract_entity_envelopes(payload, ref.as_posix())
    except InvalidFeed:
        print("invalid_protobuf", file=sys.stderr)
        return 1
    except ValueError:
        print("payload_ref_mismatch", file=sys.stderr)
        return 1

    # Only aggregate results are printed: no entity IDs or coordinates.
    kinds = Counter(",".join(row.message_fields) or "none" for row in rows)
    print(f"entity_rows={len(rows)}")
    print(f"unique_row_keys={len({row.row_key for row in rows})}")
    print(f"message_fields={dict(kinds)}")
    print(
        f"missing_feed_timestamp={sum(row.feed_timestamp_utc is None for row in rows)}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
