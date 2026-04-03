
{% test incremental_load_check(model, column_name) %}
SELECT {{ column_name }}, COUNT(*) FROM {{ model }}
WHERE CAST({{ column_name }} AS DATE) = CURRENT_DATE - 2
GROUP BY {{ column_name }}
HAVING COUNT(*) = 0
{% endtest %}