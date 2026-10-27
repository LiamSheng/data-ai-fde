"""Inspect the header and entity types of a saved GTFS-Realtime payload."""

import argparse
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from google.protobuf.message import DecodeError
from google.transit import gtfs_realtime_pb2


class InvalidFeed(ValueError):
    """The bytes cannot be interpreted as a usable GTFS-Realtime message."""


@dataclass(frozen=True, slots=True)
class FeedMetadata:
    version: str
    feed_timestamp: datetime | None
    incrementality: str
    incrementality_explicit: bool
    entity_count: int
    vehicle_count: int
    trip_update_count: int
    alert_count: int


def parse_feed(payload: bytes) -> FeedMetadata:
    """Parse bytes without inventing a timestamp when the header omits it."""
    feed = gtfs_realtime_pb2.FeedMessage()

    try:
        feed.ParseFromString(payload)
    except DecodeError:
        raise InvalidFeed("Protobuf decoding failed") from None

    # Protobuf decoding alone does not guarantee that required fields exist.
    if not feed.IsInitialized() or not feed.header.gtfs_realtime_version:
        raise InvalidFeed("required GTFS-Realtime fields are missing")

    header = feed.header

    # The specification makes timestamp optional. Missing is different from zero.
    feed_timestamp = None
    if header.HasField("timestamp"):
        try:
            feed_timestamp = datetime.fromtimestamp(header.timestamp, tz=UTC)
        except (OverflowError, OSError, ValueError):
            raise InvalidFeed("feed timestamp is out of range") from None

    # When omitted, Protobuf defines FULL_DATASET as the default. Keep track
    # of whether the producer actually supplied the field.
    try:
        incrementality = gtfs_realtime_pb2.FeedHeader.Incrementality.Name(
            header.incrementality
        )
    except ValueError:
        raise InvalidFeed("unknown incrementality value") from None

    return FeedMetadata(
        version=header.gtfs_realtime_version,
        feed_timestamp=feed_timestamp,
        incrementality=incrementality,
        incrementality_explicit=header.HasField("incrementality"),
        entity_count=len(feed.entity),
        vehicle_count=sum(entity.HasField("vehicle") for entity in feed.entity),
        trip_update_count=sum(entity.HasField("trip_update") for entity in feed.entity),
        alert_count=sum(entity.HasField("alert") for entity in feed.entity),
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Inspect one saved GTFS-RT payload")
    parser.add_argument("payload_path", type=Path)
    args = parser.parse_args()

    try:
        payload = args.payload_path.read_bytes()
    except OSError:
        print("storage_error: cannot read payload", file=sys.stderr)
        return 1

    try:
        metadata = parse_feed(payload)
    except InvalidFeed:
        print("invalid_protobuf", file=sys.stderr)
        return 1

    timestamp_text = (
        metadata.feed_timestamp.isoformat().replace("+00:00", "Z")
        if metadata.feed_timestamp is not None
        else "missing"
    )

    print(f"gtfs_realtime_version={metadata.version}")
    print(f"feed_timestamp_utc={timestamp_text}")
    print(f"incrementality={metadata.incrementality}")
    print(f"incrementality_explicit={metadata.incrementality_explicit}")
    print(f"entity_count={metadata.entity_count}")
    print(f"vehicle_count={metadata.vehicle_count}")
    print(f"trip_update_count={metadata.trip_update_count}")
    print(f"alert_count={metadata.alert_count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
