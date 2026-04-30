{% test duplicate_id_check(model, column_name) %}
SELECT {{ column_name }}
FROM {{ model }}
GROUP BY {{ column_name }}
HAVING COUNT(*) > 1
{% endtest %}