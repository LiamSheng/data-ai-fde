select
    a.attempt_id, a.poll_cycle_id, a.payload_sha256,
    a.feed_timestamp, a.completed_at_utc, a.feed_age_seconds,
    a.incrementality, a.incrementality = 'FULL_DATASET' as snapshot_supported,
    a.entity_count as source_entity_count,
    count(o.entity_index) as active_vehicle_record_count,
    case when a.incrementality = 'FULL_DATASET'
        then count(distinct o.vehicle_id) end as observed_vehicle_count,
    sum(case when o.entity_index is not null and o.vehicle_id is null then 1 else 0 end) as missing_vehicle_id_records,
    sum(case when o.entity_index is not null and o.route_id is null then 1 else 0 end) as missing_route_id_records,
    false as freshness_sla_evaluated,
    'unverified_reference' as static_linkage_status
from {{ ref('stg_capture_attempts') }} a
left join {{ ref('int_vehicle_observations') }} o on a.attempt_id = o.attempt_id
where a.feed_type = 'vehicle_positions' and a.is_success
group by
    a.attempt_id, a.poll_cycle_id, a.payload_sha256,
    a.feed_timestamp, a.completed_at_utc, a.feed_age_seconds,
    a.incrementality, a.entity_count
