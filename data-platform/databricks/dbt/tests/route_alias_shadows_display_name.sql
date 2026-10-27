-- route_delay_summary matches a Route display name and the aliases together, so an
-- alias spelled like another route's display name silently adds that route to the
-- summary. Return each alias that names a route it is not mapped to.
with aliases as (
    select lower(trim(alias)) as alias_key, route_id
    from {{ ref('route_aliases') }}
)
select a.alias_key, d.route_id as shadowed_route_id, d.route_display_name
from (select distinct alias_key from aliases) a
join {{ ref('dim_routes') }} d on a.alias_key = lower(d.route_display_name)
where not exists (
    select 1 from aliases m
    where m.alias_key = a.alias_key and m.route_id = d.route_id
)
