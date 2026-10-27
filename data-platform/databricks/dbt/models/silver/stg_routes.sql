select
    r.static_payload_sha256, r.route_id, r.agency_id,
    r.route_short_name, r.route_long_name,
    try_cast(r.route_type as int) as route_type
from {{ source('bronze', 'routes') }} r
where exists (
    select 1 from {{ ref('stg_capture_attempts') }} a
    where a.feed_type = 'gtfs_schedule' and a.is_success
        and a.payload_sha256 = r.static_payload_sha256
)
