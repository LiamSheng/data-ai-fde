{# Day type a service date has by its day of the week alone (CONTEXT.md: Day type). #}
{% macro day_of_week_type(service_date) -%}
    case date_format({{ service_date }}, 'EEE')
        when 'Sat' then 'saturday' when 'Sun' then 'sunday' else 'weekday' end
{%- endmacro %}
