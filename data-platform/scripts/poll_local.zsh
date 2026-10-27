#!/usr/bin/env zsh

# Local development only: read the secret when the poller runs, not on `cd`.
emulate -L zsh
set -e

project_root=${0:A:h:h}
cd -- "$project_root"

if ! api_key="$(security find-generic-password \
  -a "$USER" \
  -s "transit-realtime-pipeline/gtfs-rt-api-key" \
  -w)"; then
  print -u2 "Could not read the API key from macOS Keychain."
  exit 2
fi

if [[ -z "$api_key" ]]; then
  print -u2 "The API key stored in macOS Keychain is empty."
  exit 2
fi

# These are public, keyless endpoint URLs. The Python request adds the key.
GTFS_RT_API_KEY="$api_key" \
GTFS_RT_VEHICLE_POSITIONS_URL="https://gtfsapi.translink.ca/v3/gtfsposition" \
GTFS_RT_TRIP_UPDATES_URL="https://gtfsapi.translink.ca/v3/gtfsrealtime" \
GTFS_RT_SERVICE_ALERTS_URL="https://gtfsapi.translink.ca/v3/gtfsalerts" \
uv run python -m transit_realtime_pipeline.poller
