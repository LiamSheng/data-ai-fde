select
    d.static_payload_sha256, d.service_id,
    -- A malformed date stays null, so the not_null test reports it instead of
    -- failing the build.
    try_to_date(d.date, 'yyyyMMdd') as service_date,
    try_cast(d.exception_type as int) as exception_type
from {{ source('bronze', 'calendar_dates') }} d
where exists (
    select 1 from {{ ref('stg_capture_attempts') }} a
    where a.feed_type = 'gtfs_schedule' and a.is_success
        and a.payload_sha256 = d.static_payload_sha256
)
