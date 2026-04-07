{{ config(materialized = "incremental", unique_key = "surrogate_id") }}

SELECT * EXCEPT(rnum) FROM(
    SELECT
        *, row_number() over(partition by surrogate_id) AS rnum
    FROM {{ source('gh_events', 'raw_gh_events_ext') }}
    WHERE id IS NOT NULL
)
WHERE rnum = 1
{% if is_incremental() %}
    AND CAST(created_at AS DATE) = CURRENT_DATE - 2
 ---(SELECT TIMESTAMP_SUB(MAX(created_at), INTERVAL 1 HOUR) FROM {{ this }})
{% endif %}





