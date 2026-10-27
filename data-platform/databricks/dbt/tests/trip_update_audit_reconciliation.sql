-- Every successful Trip Updates audit must have its payload in Bronze with the
-- audited entity and trip-update counts (no lost entities, no explode duplicates).
-- An empty feed (entity_count 0, seen on 2026-10-20) has no rows to store, so a
-- payload missing from Bronze counts as zero entities.
with audits as (
    select
        attempt_id, payload_sha256, entity_count,
        try_cast(get_json_object(raw_audit_json, '$.trip_update_count') as bigint)
            as trip_update_count
    from {{ source('bronze', 'capture_attempts') }}
    where feed_type = 'trip_updates' and outcome = 'success'
),
bronze_counts as (
    select
        payload_sha256,
        count(distinct entity_index) as entity_count,
        count(distinct case when has_trip_update then entity_index end)
            as trip_update_count
    from {{ source('bronze', 'trip_updates') }}
    group by payload_sha256
)
select a.attempt_id, a.entity_count, c.entity_count as bronze_entity_count,
    a.trip_update_count, c.trip_update_count as bronze_trip_update_count
from audits a
left join bronze_counts c on a.payload_sha256 = c.payload_sha256
where a.entity_count is distinct from coalesce(c.entity_count, 0)
    or a.trip_update_count is distinct from coalesce(c.trip_update_count, 0)
