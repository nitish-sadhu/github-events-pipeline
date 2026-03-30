{{ config(
        materialized = "incremental",
        unique_key = "id",
        partition_by = {
            "field": "created_date",
            "data_type": "date"
        },
        cluster_by = ["type"]
) }}

SELECT
    {{
        dbt_utils.generate_surrogate_key([
            'id',
            'type',
            'public',
            'created_at'
    ])
    }} AS surrogate_id,
    id,
    type,
    actor.id as actor_id,
    repo.id as repo_id,
    org.id as org_id,
    public,
    created_at,
    DATE(created_at) AS created_date
FROM {{ ref("raw_gh_events") }}
WHERE id IS NOT NULL
{% if is_incremental() %}
    AND created_at >= (
        SELECT TIMESTAMP_SUB(MAX(created_at), INTERVAL 1 HOUR)
        FROM {{ this }}
    )
{% endif %}
