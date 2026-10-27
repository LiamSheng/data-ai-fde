{{ config(enabled=var('verify_sample', true)) }}

with measurements as (
    select 'audit_attempts' as measurement, count(*) as actual, 2 as expected
    from {{ source('bronze', 'capture_attempts') }}
    where attempt_id in ('{{ var("sample_rt_attempt_id") }}', '{{ var("sample_static_attempt_id") }}')
    union all
    select 'rt_entities', count(*), 779 from {{ source('bronze', 'vehicle_positions') }}
    where payload_sha256 = '{{ validated_sha256(var("sample_rt_sha256")) }}'
    union all
    select 'static_routes', count(*), 239 from {{ source('bronze', 'routes') }}
    where static_payload_sha256 = '{{ validated_sha256(var("reference_static_sha256")) }}'
    union all
    select 'observed_vehicles', coalesce(max(observed_vehicle_count), -1), 779
    from {{ ref('snapshot_status') }} where attempt_id = '{{ var("sample_rt_attempt_id") }}'
    union all
    select 'route_vehicle_sum', coalesce(sum(observed_vehicle_count), 0), 779
    from {{ ref('route_vehicle_counts') }} where attempt_id = '{{ var("sample_rt_attempt_id") }}'
)
select * from measurements where actual != expected
