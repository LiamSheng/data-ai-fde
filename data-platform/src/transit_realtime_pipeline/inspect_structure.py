"""Show the fields present in a saved GTFS-Realtime payload.

This is an inspection tool, not a transformation. It prints field names,
types, and repeated-field counts, but never prints IDs or coordinates.
"""

import argparse
import sys
from pathlib import Path

from google.protobuf.descriptor import FieldDescriptor
from google.protobuf.message import Message
from google.transit import gtfs_realtime_pb2

from .gtfs_rt import InvalidFeed, parse_feed

SCALAR_TYPES = {
    FieldDescriptor.TYPE_DOUBLE: "double",
    FieldDescriptor.TYPE_FLOAT: "float",
    FieldDescriptor.TYPE_INT64: "int64",
    FieldDescriptor.TYPE_UINT64: "uint64",
    FieldDescriptor.TYPE_INT32: "int32",
    FieldDescriptor.TYPE_FIXED64: "fixed64",
    FieldDescriptor.TYPE_FIXED32: "fixed32",
    FieldDescriptor.TYPE_BOOL: "bool",
    FieldDescriptor.TYPE_STRING: "string",
    FieldDescriptor.TYPE_BYTES: "bytes",
    FieldDescriptor.TYPE_UINT32: "uint32",
    FieldDescriptor.TYPE_SFIXED32: "sfixed32",
    FieldDescriptor.TYPE_SFIXED64: "sfixed64",
    FieldDescriptor.TYPE_SINT32: "sint32",
    FieldDescriptor.TYPE_SINT64: "sint64",
}


def field_type_name(field: FieldDescriptor) -> str:
    """Translate a Protobuf field descriptor into a readable type."""
    if field.type == FieldDescriptor.TYPE_MESSAGE:
        assert field.message_type is not None
        return field.message_type.name
    if field.type == FieldDescriptor.TYPE_ENUM:
        assert field.enum_type is not None
        return f"enum {field.enum_type.name}"
    return SCALAR_TYPES.get(field.type, "unknown scalar")


def show_structure(message: Message, label: str, level: int = 0) -> None:
    """Recursively show present fields, sampling one item per repeated message."""
    print(f"{'  ' * level}{label} ({message.DESCRIPTOR.name})")

    # ListFields reports fields present in this particular message.
    # It does not list every field allowed by the GTFS-Realtime schema.
    for field, value in message.ListFields():
        indent = "  " * (level + 1)
        kind = field_type_name(field)

        if field.is_repeated:
            print(f"{indent}{field.name}: repeated {kind}, count={len(value)}")

            # Show the shape of the first item without printing hundreds
            # of near-identical entities or any individual field values.
            if field.type == FieldDescriptor.TYPE_MESSAGE and value:
                show_structure(value[0], f"{field.name}[0]", level + 2)

        elif field.type == FieldDescriptor.TYPE_MESSAGE:
            show_structure(value, field.name, level + 1)

        else:
            print(f"{indent}{field.name}: {kind} [present]")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Show the structure of one saved GTFS-Realtime payload"
    )
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

    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(payload)

    print(f"payload_bytes={len(payload)}")
    print(f"entity_count={metadata.entity_count}")
    show_structure(feed, "feed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
