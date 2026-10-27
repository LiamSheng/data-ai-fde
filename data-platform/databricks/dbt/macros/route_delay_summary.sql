{#
  route_delay_summary: the trusted answer to "how late is route X on day type Y between
  hours A and B" (ADR 0001). It only filters, applies the Sufficient evidence gate and
  aggregates; every measurement rule lives in fct_trip_delay_sample. Deployed after
  dbt build by `dbt run-operation deploy_route_delay_summary`; tested offline by
  `dev/duckdb_check.py function`, which compiles route_delay_summary_ddl() and runs
  its body on DuckDB with literal arguments.
#}

{% macro deploy_route_delay_summary() %}
    {% do run_query(route_delay_summary_ddl()) %}
    {% do log("Deployed " ~ route_delay_summary_name(), info=true) %}
{% endmacro %}

{% macro route_delay_summary_name() -%}
    {%- set fct = ref('fct_trip_delay_sample') -%}
    {{ fct.database }}.{{ fct.schema }}.route_delay_summary
{%- endmacro %}

{% macro route_delay_summary_ddl() -%}
create or replace function {{ route_delay_summary_name() }}(
    route string comment 'Route display name such as 99 or Expo Line, or an alias in route_aliases such as B-Line',
    day_type string comment 'weekday, saturday or sunday',
    start_hour int comment 'First America/Vancouver observation hour, included',
    end_hour int comment 'Observation hour where the window ends, excluded',
    min_trips int default {{ var('sufficient_evidence_min_trips') | int }}
        comment 'Sufficient evidence: distinct trips needed',
    min_service_dates int default {{ var('sufficient_evidence_min_service_dates') | int }}
        comment 'Sufficient evidence: service dates needed'
)
returns table (
    route_display_name string,
    day_type string,
    start_hour int,
    end_hour int,
    sample_count bigint,
    trip_count bigint,
    service_date_count bigint,
    first_service_date date,
    last_service_date date,
    is_sufficient boolean,
    median_delay_seconds int,
    p90_delay_seconds int,
    late_sample_share double
)
comment 'Current delay summary of a route by day type and observation hours; percentiles and late share only with Sufficient evidence'
return
{{ route_delay_summary_query() }}
{%- endmacro %}

{#
  Parameters are named route_delay_summary.<name>: Databricks resolves an unqualified
  name to a column first, and day_type is also a column of the fact.
#}
{% macro route_delay_summary_query() -%}
{%- set param = 'route_delay_summary.' -%}
with request as (
    select
        lower(trim({{ param }}route)) as route_key,
        lower(trim({{ param }}day_type)) as day_type
),
-- A display name matches without leading zeros ("099" is "99"), an alias as written;
-- both ignore case and surrounding spaces.
routes as (
    select d.route_id, d.route_display_name
    from {{ ref('dim_routes') }} d
    cross join request q
    where lower(d.route_display_name)
        = coalesce(nullif(trim(LEADING '0' FROM q.route_key), ''), q.route_key)
    union
    select d.route_id, d.route_display_name
    from {{ ref('route_aliases') }} a
    join {{ ref('dim_routes') }} d on a.route_id = d.route_id
    cross join request q
    where lower(trim(a.alias)) = q.route_key
),
samples as (
    select s.*
    from {{ ref('fct_trip_delay_sample') }} s
    join routes r on s.route_id = r.route_id
    cross join request q
    where s.day_type = q.day_type
        and s.observation_hour >= {{ param }}start_hour
        and s.observation_hour < {{ param }}end_hour
),
evidence as (
    select
        count(*) as sample_count,
        -- A trip is a trip_id on one service date; the same trip_id runs every day.
        count(distinct service_date, trip_id) as trip_count,
        count(distinct service_date) as service_date_count,
        min(service_date) as first_service_date,
        max(service_date) as last_service_date,
        -- Suspect delays count as samples but are never ranked.
        percentile_approx(
            case when not is_suspect_delay then current_delay_seconds end, 0.5
        ) as median_delay_seconds,
        percentile_approx(
            case when not is_suspect_delay then current_delay_seconds end, 0.9
        ) as p90_delay_seconds,
        avg(case when is_late then 1 else 0 end) as late_sample_share
    from samples
),
judged as (
    select
        e.*,
        e.trip_count >= {{ param }}min_trips
            and e.service_date_count >= {{ param }}min_service_dates as is_sufficient
    from evidence e
)
select
    (
        select nullif(array_join(sort_array(collect_set(route_display_name)), ', '), '')
        from routes
    ) as route_display_name,
    (select day_type from request) as day_type,
    {{ param }}start_hour as start_hour,
    {{ param }}end_hour as end_hour,
    j.sample_count, j.trip_count, j.service_date_count,
    j.first_service_date, j.last_service_date,
    j.is_sufficient,
    case when j.is_sufficient then j.median_delay_seconds end as median_delay_seconds,
    case when j.is_sufficient then j.p90_delay_seconds end as p90_delay_seconds,
    case when j.is_sufficient then j.late_sample_share end as late_sample_share
from judged j
{%- endmacro %}
