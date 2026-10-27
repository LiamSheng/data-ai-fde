"""Read keyless endpoint configuration for a named GTFS-Realtime feed."""

import os
from dataclasses import dataclass
from urllib.parse import urlsplit

FEED_URL_ENV_VARS = {
    "vehicle_positions": "GTFS_RT_VEHICLE_POSITIONS_URL",
    "trip_updates": "GTFS_RT_TRIP_UPDATES_URL",
    "service_alerts": "GTFS_RT_SERVICE_ALERTS_URL",
}
FEED_TYPES = tuple(FEED_URL_ENV_VARS)


@dataclass(frozen=True, slots=True)
class SourceConfig:
    """Backward-compatible configuration for the single-position entry point."""

    api_key: str
    vehicle_positions_url: str


@dataclass(frozen=True, slots=True)
class FeedConfig:
    api_key: str
    url: str


def load_feed_config(feed_type: str) -> FeedConfig:
    """Read one feed's URL; never include supplied values in errors."""
    if feed_type not in FEED_URL_ENV_VARS:
        raise ValueError("unknown feed type")

    api_key = os.getenv("GTFS_RT_API_KEY")
    url = os.getenv(FEED_URL_ENV_VARS[feed_type])

    # Never include the supplied value in an error message.
    if not api_key or api_key != api_key.strip():
        raise ValueError("GTFS_RT_API_KEY is missing or has surrounding whitespace")
    if not url:
        raise ValueError("feed URL is missing")

    # Keep credentials out of the configured URL. The request adds the key later.
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
        raise ValueError("feed URL must be a keyless HTTPS endpoint")

    return FeedConfig(api_key=api_key, url=url)


def load_config() -> SourceConfig:
    """Retain the existing single-feed configuration interface."""
    feed = load_feed_config("vehicle_positions")
    return SourceConfig(api_key=feed.api_key, vehicle_positions_url=feed.url)
