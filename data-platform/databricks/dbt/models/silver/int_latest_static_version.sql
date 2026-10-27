-- The Static version that labels and classifies samples: the one whose successful
-- capture completed last among versions whose content is in Bronze. Ties go to the
-- smallest hash, as ingestion picks them.
select payload_sha256
from {{ ref('stg_capture_attempts') }} a
where feed_type = 'gtfs_schedule' and is_success
    and exists (
        select 1 from {{ ref('stg_routes') }} r
        where r.static_payload_sha256 = a.payload_sha256
    )
order by completed_at_utc desc, payload_sha256
limit 1
