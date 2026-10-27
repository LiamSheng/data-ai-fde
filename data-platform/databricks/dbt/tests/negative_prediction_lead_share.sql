-- Delay samples drop a departed trip whose Next stop prediction is already past the
-- feed timestamp (lead below delay_sample_min_lead_seconds). Warn when that drops more
-- than negative_lead_share_warn of departed Next stops, so the bound gets revisited on
-- real data. Next stop selection mirrors fct_trip_delay_sample.
{{ config(severity='warn') }}

with next_stops as (
    select
        stop_sequence,
        timestampdiff(SECOND, feed_timestamp, arrival_time) as prediction_lead_seconds,
        row_number() over (
            partition by payload_sha256, entity_index
            order by stop_sequence, update_index
        ) as stop_rank
    from {{ ref('stg_trip_updates') }}
    where trip_schedule_relationship in ('SCHEDULED', 'DUPLICATED')
        and stop_schedule_relationship = 'SCHEDULED'
        and stop_sequence is not null
        and arrival_time is not null
        and arrival_delay is not null
),
share as (
    select
        count(*) as departed_next_stops,
        avg(case when prediction_lead_seconds
            < {{ var('delay_sample_min_lead_seconds') | int }} then 1.0 else 0.0 end
        ) as negative_lead_share
    from next_stops
    where stop_rank = 1 and stop_sequence > 1
)
select * from share
where departed_next_stops > 0
    and negative_lead_share > {{ var('negative_lead_share_warn') | float }}
