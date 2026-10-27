"""Summarize field coverage in one saved Vehicle Positions payload."""

import argparse
import json
import sys
from dataclasses import asdict, dataclass
from hashlib import sha256
from pathlib import Path

from google.transit import gtfs_realtime_pb2

from transit_realtime_pipeline.gtfs_rt import InvalidFeed, parse_feed


@dataclass(frozen=True, slots=True)
class VehicleProfile:
    payload_sha256: str
    payload_bytes: int
    entity_count: int
    vehicle_entity_count: int
    duplicate_entity_id_occurrences: int
    vehicle_id_present: int
    trip_id_present: int
    route_id_present: int
    position_present: int
    vehicle_timestamp_present: int


def profile_payload(payload: bytes) -> VehicleProfile:
    """Count field presence; never expose vehicle IDs or coordinates."""
    # Reuse our existing validation before examining individual entities.
    metadata = parse_feed(payload)

    # Parsing a second time is acceptable for this small, offline inspection.
    # Later, if profiling becomes part of the pipeline, we can share one parse.
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(payload)

    vehicles = [entity.vehicle for entity in feed.entity if entity.HasField("vehicle")]

    # Count repeated entity IDs without printing the IDs themselves.
    distinct_entity_ids = {entity.id for entity in feed.entity}

    return VehicleProfile(
        payload_sha256=sha256(payload).hexdigest(),
        payload_bytes=len(payload),
        entity_count=metadata.entity_count,
        vehicle_entity_count=len(vehicles),
        duplicate_entity_id_occurrences=(
            metadata.entity_count - len(distinct_entity_ids)
        ),
        vehicle_id_present=sum(bool(v.vehicle.id) for v in vehicles),
        trip_id_present=sum(bool(v.trip.trip_id) for v in vehicles),
        route_id_present=sum(bool(v.trip.route_id) for v in vehicles),
        position_present=sum(v.HasField("position") for v in vehicles),
        vehicle_timestamp_present=sum(v.HasField("timestamp") for v in vehicles),
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Profile one saved Vehicle Positions payload"
    )
    parser.add_argument("payload_path", type=Path)
    args = parser.parse_args()

    try:
        payload = args.payload_path.read_bytes()
    except OSError:
        print("storage_error: cannot read payload", file=sys.stderr)
        return 1

    try:
        profile = profile_payload(payload)
    except InvalidFeed:
        print("invalid_protobuf", file=sys.stderr)
        return 1

    print(json.dumps(asdict(profile), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
