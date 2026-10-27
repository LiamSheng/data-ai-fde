-- Delay samples treat stop_sequence 1 as the origin. If the feed renumbers stops
-- (e.g. starting at 10), almost no trip is seen at sequence 1, origin samples slip
-- past the departed filter, and results drift toward on-time. Fail instead.
-- Service dates seen by few trips (e.g. only their after-midnight tail at the edge of
-- the captured range) are mostly mid-route and are not judged.
with trips as (
    select
        service_date, trip_id,
        max(case when stop_sequence = 1 then 1 else 0 end) as seen_at_origin
    from {{ ref('stg_trip_updates') }}
    where has_trip_update and trip_schedule_relationship != 'CANCELED'
        and stop_sequence is not null
    group by service_date, trip_id
)
select
    service_date, count(*) as trips,
    avg(seen_at_origin) as origin_observed_share
from trips
group by service_date
having count(*) >= {{ var('origin_drift_min_trips') | int }}
    and avg(seen_at_origin) < {{ var('origin_observed_trip_share_floor') | float }}
