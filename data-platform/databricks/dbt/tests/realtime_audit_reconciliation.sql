-- Every successful Vehicle Positions and Service Alerts audit must have its payload
-- in Bronze with the audited entity and entity-type counts (no lost entities, no
-- duplicates). Replaces the fixed-sample sample_reconciliation (KAN-36). An empty
-- feed has no rows to store, so a payload missing from Bronze counts as zero.
with audits as (
    select
        attempt_id, feed_type, payload_sha256, entity_count,
        case feed_type
            when 'vehicle_positions' then vehicle_count
            else try_cast(get_json_object(raw_audit_json, '$.alert_count') as bigint)
        end as typed_count
    from {{ source('bronze', 'capture_attempts') }}
    where feed_type in ('vehicle_positions', 'service_alerts') and outcome = 'success'
),
bronze_counts as (
    select
        'vehicle_positions' as feed_type, payload_sha256,
        count(distinct entity_index) as entity_count,
        count(distinct case when has_vehicle then entity_index end) as typed_count
    from {{ source('bronze', 'vehicle_positions') }}
    group by payload_sha256
    union all
    select
        'service_alerts', payload_sha256,
        count(distinct entity_index),
        count(distinct case when has_alert then entity_index end)
    from {{ source('bronze', 'service_alerts') }}
    group by payload_sha256
)
select a.attempt_id, a.feed_type, a.entity_count, c.entity_count as bronze_entity_count,
    a.typed_count, c.typed_count as bronze_typed_count
from audits a
left join bronze_counts c
    on a.feed_type = c.feed_type and a.payload_sha256 = c.payload_sha256
-- vehicle_count is optional in the audit contract (alert_count is required), so a
-- missing typed count is not compared.
where a.entity_count is distinct from coalesce(c.entity_count, 0)
    or (a.typed_count is not null
        and a.typed_count is distinct from coalesce(c.typed_count, 0))
