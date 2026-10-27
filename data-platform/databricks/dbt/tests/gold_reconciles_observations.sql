select s.attempt_id
from {{ ref('snapshot_status') }} s
left join (
    select attempt_id, sum(observed_vehicle_count) as route_vehicle_sum
    from {{ ref('route_vehicle_counts') }} group by attempt_id
) g on s.attempt_id = g.attempt_id
where s.snapshot_supported
    and coalesce(g.route_vehicle_sum, 0) != s.observed_vehicle_count
