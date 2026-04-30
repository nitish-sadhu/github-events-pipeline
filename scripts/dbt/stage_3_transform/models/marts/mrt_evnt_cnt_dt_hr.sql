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
WHERE created_date = CURRENT_DATE - 2
{% endif %}

GROUP BY 1, 2, 3
