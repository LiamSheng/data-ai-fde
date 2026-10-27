-- Samples whose route_id is missing or absent from the route dimension stay in the
-- fact table without a Route display name, so route questions silently miss them.
-- A row is returned only above route_unmatched_share_warn; fail_calc then scores it
-- 1 (warn) or, above route_unmatched_share_error, 2 (error).
{{ config(
    fail_calc="coalesce(max(case when unmatched_share > "
        ~ (var('route_unmatched_share_error') | float)
        ~ " then 2 else 1 end), 0)",
    warn_if=">=1",
    error_if=">=2",
) }}

with share as (
    select
        count(*) as samples,
        sum(case when route_display_name is null then 1 else 0 end) as unmatched_samples,
        avg(case when route_display_name is null then 1.0 else 0.0 end) as unmatched_share
    from {{ ref('fct_trip_delay_sample') }}
)
select * from share
where samples > 0
    and unmatched_share > {{ var('route_unmatched_share_warn') | float }}
