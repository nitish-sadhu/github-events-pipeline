{{ config(materialized = "incremental", unique_key = "id") }}

SELECT
    *
FROM {{ source('gh_archives', 'raw_gh_events_ext') }}
WHERE id IS NOT NULL
{% if is_incremental() %}
    AND created_at >= (SELECT TIMESTAMP_SUB(MAX(created_at), INTERVAL 1 HOUR) FROM {{ this }})
{% endif %}





