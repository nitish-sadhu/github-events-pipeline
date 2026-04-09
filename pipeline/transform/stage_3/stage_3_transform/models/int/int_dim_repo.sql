{{ config(materialized = "incremental",
            unique_key = "") }}
/*
SELECT
    DISTINCT
    {{
        dbt_utils.generate_surrogate_key([
            'repo.id',
            'repo.name',
            'repo.url'
    ])
    }} as surrogate_id,
    repo.id AS id,
    repo.name AS name,
    repo.url as url
FROM {{ ref("raw_gh_events") }}
WHERE repo.id IS NOT NULL
*/

WITH cte_repo AS (
    SELECT
        {{
            dbt_utils.generate_surrogate_key([
                'repo.id',
                'repo.name',
                'repo.url'
        ])
        }} as surrogate_id,
        repo.id AS id,
        repo.name AS name,
        repo.url as url,
    FROM {{ ref("raw_gh_events") }}
    WHERE repo.id IS NOT NULL

)

SELECT DISTINCT * FROM cte_repo


