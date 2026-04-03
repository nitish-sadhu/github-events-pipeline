{% test duplicate_id_check(model, id_col) %}
SELECT {{ id_col }}, COUNT(*) FROM {{ model }}
GROUP BY {{ id_col }}
HAVING COUNT(*) > 1
{% endtest %}