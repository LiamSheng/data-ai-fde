-- Service dates whose Day type differs from their day of the week, read from the
-- latest Static calendar (CONTEXT.md: Day type). A single Day type service runs only
-- Monday-Friday, only Saturday or only Sunday; on a calendar exception date the single
-- Day type services still running (not removed, or added) give the date's Day type
-- when they agree. TransLink, for example, swaps weekday for Sunday service on holidays.
with calendar as (
    select
        c.service_id, c.start_date, c.end_date,
        case
            when c.monday and c.tuesday and c.wednesday and c.thursday and c.friday
                and not c.saturday and not c.sunday then 'weekday'
            when c.saturday and not (c.monday or c.tuesday or c.wednesday
                or c.thursday or c.friday or c.sunday) then 'saturday'
            when c.sunday and not (c.monday or c.tuesday or c.wednesday
                or c.thursday or c.friday or c.saturday) then 'sunday'
        end as day_type
    from {{ ref('stg_calendar') }} c
    join {{ ref('int_latest_static_version') }} v
        on c.static_payload_sha256 = v.payload_sha256
),
exceptions as (
    select d.service_id, d.service_date, d.exception_type
    from {{ ref('stg_calendar_dates') }} d
    join {{ ref('int_latest_static_version') }} v
        on d.static_payload_sha256 = v.payload_sha256
),
running as (
    select e.service_date, c.day_type
    from (select distinct service_date from exceptions) e
    cross join calendar c
    left join exceptions x
        on x.service_id = c.service_id and x.service_date = e.service_date
    where c.day_type is not null
        and (
            x.exception_type = 1
            or (
                x.exception_type is null
                and e.service_date between c.start_date and c.end_date
                and c.day_type = {{ day_of_week_type('e.service_date') }}
            )
        )
)
select service_date, min(day_type) as day_type
from running
group by service_date
having count(distinct day_type) = 1
    and min(day_type) != {{ day_of_week_type('service_date') }}
