select
    a.attempt_id, a.poll_cycle_id, a.payload_sha256,
    a.started_at_utc, a.completed_at_utc,
    a.feed_timestamp, a.feed_age_seconds,
    a.incrementality = 'FULL_DATASET' as is_full_snapshot,
    v.entity_index, v.entity_id, v.vehicle_id, v.trip_id, v.route_id,
    v.vehicle_timestamp, v.latitude, v.longitude
from {{ ref('stg_capture_attempts') }} a
inner join {{ ref('stg_vehicle_positions') }} v
    on a.payload_sha256 = v.payload_sha256
where a.feed_type = 'vehicle_positions' and a.is_success
    and v.has_vehicle and not coalesce(v.is_deleted, false)
