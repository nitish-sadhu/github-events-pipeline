{{ config(materialized = "incremental",
        unique_key = ["created_date", "created_hour", "event_type"],
        partition_by = {
                        "field": "created_date",
                        "data_type": "date"
} )}}

SELECT
    created_date,
    EXTRACT(HOUR FROM created_at) as created_hour,
    type as event_type,
    COUNT(*) AS event_counts
FROM {{ ref("int_fct_gh_events") }}

{% if is_incremental() %}
WHERE created_date >= (
        SELECT DATE_SUB(MAX(created_date), INTERVAL 2 DAY)
        FROM {{ this }}
    )
{% endif %}

GROUP BY 1, 2, 3
