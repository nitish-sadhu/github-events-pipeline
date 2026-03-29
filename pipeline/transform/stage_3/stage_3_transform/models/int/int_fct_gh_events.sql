{{ config(materialized = "incremental") }}

SELECT
    {{
        dbt_utils.generate_surrogate_key([
            'id',
            'type',
            'public',
            'created_at'
    ])
    }} AS surrogate_key
    id,
    type,
    public,
    created_at
FROM {{ ref("raw_gh_events") }}
WHERE id IS NOT NULL

