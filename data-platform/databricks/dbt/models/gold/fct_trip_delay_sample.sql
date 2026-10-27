{{ config(unique_key=['payload_sha256', 'entity_index']) }}

-- One Delay sample per snapshot and scheduled (or duplicated) trip, measured at its
-- Next stop (terms in CONTEXT.md). A snapshot is a distinct payload, so repeated
-- captures of identical bytes add no samples. Current delay counts only once the trip
-- has departed (Next stop past the origin) and the predicted arrival is near but not
-- already past.
with next_stops as (
    select
        payload_sha256, entity_index, feed_timestamp, service_date,
        trip_id, route_id, direction_id, vehicle_id,
        stop_sequence, stop_id, arrival_delay,
        timestampdiff(SECOND, feed_timestamp, arrival_time) as prediction_lead_seconds,
        row_number() over (
            partition by payload_sha256, entity_index
            order by stop_sequence, update_index
        ) as stop_rank
    from {{ ref('stg_trip_updates') }}
    -- Canceled and deleted trips are withdrawn; added trips have no schedule to be
    -- late against.
    where trip_schedule_relationship in ('SCHEDULED', 'DUPLICATED')
        and stop_schedule_relationship = 'SCHEDULED'
        and stop_sequence is not null
        and arrival_time is not null
        and arrival_delay is not null
)
select
    n.payload_sha256, n.entity_index, n.feed_timestamp, n.service_date,
    -- Day type comes from the service date, never the observation date: the Static
    -- calendar's override first (holidays run as sunday), otherwise the day of the week.
    coalesce(o.day_type, {{ day_of_week_type('n.service_date') }}) as day_type,
    -- convert_timezone renders the instant as Vancouver wall-clock time whatever
    -- the session time zone is.
    hour(convert_timezone('America/Vancouver', n.feed_timestamp)) as observation_hour,
    n.trip_id, n.route_id,
    -- Unmatched routes (including a missing route_id) keep their samples, unnamed.
    r.route_display_name,
    n.direction_id, n.vehicle_id,
    n.stop_sequence as next_stop_sequence,
    n.stop_id as next_stop_id,
    n.arrival_delay as current_delay_seconds,
    n.prediction_lead_seconds,
    n.arrival_delay >= {{ var('late_threshold_seconds') | int }} as is_late,
    -- Suspect delays are kept and flagged, never dropped, so they stay auditable.
    abs(n.arrival_delay) > {{ var('suspect_delay_limit_seconds') | int }} as is_suspect_delay
from next_stops n
left join {{ ref('service_date_overrides') }} o on n.service_date = o.service_date
left join {{ ref('dim_routes') }} r on n.route_id = r.route_id
where n.stop_rank = 1
    and n.stop_sequence > 1
    and n.prediction_lead_seconds between {{ var('delay_sample_min_lead_seconds') | int }}
        and {{ var('delay_sample_max_lead_seconds') | int }}
