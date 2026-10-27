{% test unique_key(model, columns) %}
select {{ columns | join(', ') }}, count(*) as duplicate_count
from {{ model }}
group by {{ columns | join(', ') }}
having count(*) > 1
{% endtest %}
