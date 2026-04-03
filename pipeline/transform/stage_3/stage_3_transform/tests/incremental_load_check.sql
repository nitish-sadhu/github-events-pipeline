
{% test incremental_load_check(model, date_col) %}
SELECT date_col, COUNT(*) FROM {{ model }}
WHERE CAST(date_col AS DATE) = CURRENT_DATE - 2
HAVING COUNT(*) = 0
{% endtest %}