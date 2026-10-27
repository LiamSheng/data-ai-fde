"""Collect all configured feeds once, using local or GCS persistence."""

import sys
from uuid import uuid4

from .capture import capture_one
from .config import FEED_TYPES
from .storage import configured_cloud_storage


def main() -> int:
    poll_cycle_id = uuid4().hex
    print(f"poll_cycle_id={poll_cycle_id}")

    try:
        # Initialize once, then share the same storage across the three feeds.
        storage = configured_cloud_storage()
    except Exception:  # noqa: BLE001 - redact credential/SDK exception details
        # No feed attempt has begun.
        # If persistence cannot initialize, no durable audit is promised.
        print("cycle_outcome=storage_initialization_error", file=sys.stderr)
        return 3

    outcomes: list[int] = []

    for feed_type in FEED_TYPES:
        if storage is None:
            # Preserve the existing local calling convention.
            code = capture_one(feed_type, poll_cycle_id=poll_cycle_id)
        else:
            code = capture_one(
                feed_type,
                poll_cycle_id=poll_cycle_id,
                storage=storage,
            )

        # A failed feed does not stop the remaining feeds.
        outcomes.append(code)

    failures = sum(code != 0 for code in outcomes)
    print(f"feeds_attempted={len(outcomes)}")
    print(f"feeds_failed={failures}")

    # Preserve the most serious per-feed result, including audit failure.
    return max(outcomes)


if __name__ == "__main__":
    raise SystemExit(main())
