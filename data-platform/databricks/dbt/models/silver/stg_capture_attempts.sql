select
    attempt_id, poll_cycle_id, feed_type, outcome, http_status,
    payload_ref, payload_sha256, payload_bytes,
    started_at_utc, completed_at_utc, feed_timestamp,
    feed_age_seconds as recorded_feed_age_seconds,
    case when feed_timestamp is not null then
        timestampdiff(MICROSECOND, feed_timestamp, completed_at_utc) / 1000000.0
    end as feed_age_seconds,
    incrementality, entity_count, vehicle_count, feed_version,
    audit_ref, audit_sha256,
    outcome = 'success' and http_status between 200 and 299 as is_success
from {{ source('bronze', 'capture_attempts') }}
