{{ config(unique_key=['attempt_id', 'route_id']) }}

select
    o.attempt_id, o.poll_cycle_id, o.payload_sha256,
    o.feed_timestamp, o.completed_at_utc, o.feed_age_seconds,
    o.route_id, r.route_short_name, r.route_long_name,
    '{{ validated_sha256(var("reference_static_sha256")) }}' as reference_static_sha256,
    'unverified_reference' as static_linkage_status,
    r.route_id is not null as static_route_matched,
    count(*) as vehicle_record_count,
    count(distinct o.vehicle_id) as observed_vehicle_count,
    sum(case when o.vehicle_id is null then 1 else 0 end) as missing_vehicle_id_records
from {{ ref('int_vehicle_observations') }} o
left join {{ ref('dim_reference_routes') }} r on o.route_id = r.route_id
where o.is_full_snapshot
group by
    o.attempt_id, o.poll_cycle_id, o.payload_sha256,
    o.feed_timestamp, o.completed_at_utc, o.feed_age_seconds,
    o.route_id, r.route_id, r.route_short_name, r.route_long_name
