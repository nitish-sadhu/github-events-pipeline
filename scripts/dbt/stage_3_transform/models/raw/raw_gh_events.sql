{{ config(materialized = "incremental",
            unique_key = "surrogate_id",
            partition_by={
                "field": "created_at",
                "data_type": "timestamp",
                "granularity": "day"
            })
}}


SELECT
    *
FROM {{ source('gh_events', 'raw_gh_events_ext') }}
WHERE id IS NOT NULL
{% if is_incremental() %}
    AND year = EXTRACT(YEAR FROM CURRENT_DATE - 2)
    AND month = EXTRACT(MONTH FROM CURRENT_DATE - 2)
    AND day = EXTRACT(DAY FROM CURRENT_DATE - 2)
    AND DATE(created_at) = CURRENT_DATE - 2
{% else %}
    AND year >= 2024
{% endif %}




