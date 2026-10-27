select attempt_id, vehicle_id
from {{ ref('int_vehicle_observations') }}
where vehicle_id is not null
group by attempt_id, vehicle_id
having count(distinct route_id) > 1
