select
    payload_sha256, entity_index, entity_id, feed_timestamp,
    incrementality, has_vehicle, is_deleted,
    case when trim(vehicle_id) != '' then vehicle_id end as vehicle_id,
    case when trim(trip_id) != '' then trip_id end as trip_id,
    case when trim(route_id) != '' then route_id end as route_id,
    vehicle_timestamp, latitude, longitude
from {{ source('bronze', 'vehicle_positions') }}
