{{ config(materialized = "table") }}

WITH counts_per_day AS (
    SELECT
        created_date,
        COUNT(*) AS counts
    FROM {{ ref("int_fct_gh_events") }}
    GROUP BY 1
)

SELECT
    created_date,
    counts
FROM counts_per_day
ORDER BY 1 DESC
