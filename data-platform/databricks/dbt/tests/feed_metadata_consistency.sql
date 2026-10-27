select attempt_id
from {{ ref('stg_capture_attempts') }}
where feed_type = 'vehicle_positions' and is_success and (
    feed_timestamp is null or completed_at_utc < started_at_utc
    or recorded_feed_age_seconds is null
    or abs(feed_age_seconds - recorded_feed_age_seconds) > 0.001
    or feed_age_seconds < 0
)
