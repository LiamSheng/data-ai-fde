select
    c.static_payload_sha256, c.service_id,
    c.monday = '1' as monday, c.tuesday = '1' as tuesday,
    c.wednesday = '1' as wednesday, c.thursday = '1' as thursday,
    c.friday = '1' as friday, c.saturday = '1' as saturday, c.sunday = '1' as sunday,
    try_to_date(c.start_date, 'yyyyMMdd') as start_date,
    try_to_date(c.end_date, 'yyyyMMdd') as end_date
from {{ source('bronze', 'calendar') }} c
where exists (
    select 1 from {{ ref('stg_capture_attempts') }} a
    where a.feed_type = 'gtfs_schedule' and a.is_success
        and a.payload_sha256 = c.static_payload_sha256
)
