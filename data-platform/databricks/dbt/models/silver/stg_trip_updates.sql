-- One row per Bronze Trip Updates row whose payload has a successful audit.
-- CANCELED trips and SKIPPED / NO_DATA stops stay; absent schedule relationships
-- take the GTFS-Realtime default, SCHEDULED.
select
    t.payload_sha256, t.entity_index, t.entity_id, t.feed_timestamp,
    t.incrementality, t.has_trip_update,
    case when trim(t.trip_id) != '' then t.trip_id end as trip_id,
    case when trim(t.route_id) != '' then t.route_id end as route_id,
    t.direction_id,
    try_to_date(case when trim(t.start_date) != '' then t.start_date end, 'yyyyMMdd') as service_date,
    case when t.has_trip_update
        then coalesce(t.trip_schedule_relationship, 'SCHEDULED') end as trip_schedule_relationship,
    t.trip_timestamp,
    case when trim(t.vehicle_id) != '' then t.vehicle_id end as vehicle_id,
    t.update_index, t.stop_sequence,
    case when trim(t.stop_id) != '' then t.stop_id end as stop_id,
    case when t.update_index is not null
        then coalesce(t.stop_schedule_relationship, 'SCHEDULED') end as stop_schedule_relationship,
    t.arrival_time, t.arrival_delay, t.departure_time, t.departure_delay
from {{ source('bronze', 'trip_updates') }} t
where exists (
    select 1 from {{ ref('stg_capture_attempts') }} a
    where a.feed_type = 'trip_updates' and a.is_success
        and a.payload_sha256 = t.payload_sha256
)
