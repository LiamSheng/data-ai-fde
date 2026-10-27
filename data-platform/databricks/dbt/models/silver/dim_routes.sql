-- Routes of the latest Static version in Bronze, with the Route display name
-- (CONTEXT.md): the short name without leading zeros, else the long name.
select
    r.static_payload_sha256, r.route_id,
    r.route_short_name, r.route_long_name, r.route_type,
    coalesce(
        nullif(trim(LEADING '0' FROM trim(r.route_short_name)), ''),
        nullif(trim(r.route_long_name), '')
    ) as route_display_name
from {{ ref('stg_routes') }} r
join {{ ref('int_latest_static_version') }} v on r.static_payload_sha256 = v.payload_sha256
