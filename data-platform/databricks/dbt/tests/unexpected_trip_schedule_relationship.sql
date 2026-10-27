-- Only SCHEDULED and CANCELED trips have been seen in the feed. Other relationships
-- (DUPLICATED samples; DELETED, ADDED and the rest do not) change which trips count,
-- so warn when one appears and check the sample rules still fit.
{{ config(severity='warn') }}

select trip_schedule_relationship, count(*) as stop_update_rows
from {{ ref('stg_trip_updates') }}
where has_trip_update
    and trip_schedule_relationship not in ('SCHEDULED', 'CANCELED')
group by trip_schedule_relationship
