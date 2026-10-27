select
    static_payload_sha256, route_id, agency_id,
    route_short_name, route_long_name, route_type,
    'unverified_reference' as static_linkage_status
from {{ ref('stg_routes') }}
where static_payload_sha256 = '{{ validated_sha256(var("reference_static_sha256")) }}'
