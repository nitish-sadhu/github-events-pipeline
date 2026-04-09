{{ config(materialized = "incremental",
            unique_key="id") }}
/*
SELECT
    DISTINCT
    {{
        dbt_utils.generate_surrogate_key([
            'repo.id',
            'repo.name',
    ])
    }} as surrogate_id,
    repo.id AS id,
    repo.name AS name,
    repo.url as url
FROM {{ ref("raw_gh_events") }}
WHERE repo.id IS NOT NULL AND
      repo.id <> 'None'
        AND CAST(created_at AS DATE) = (SELECT MAX(CAST(created_at AS DATE)) FROM )
*/

---SELECT ONLY THE LATEST NAME AND URL FOR THE REPO (TYPE - 1 TABLE).

WITH cte_repo AS (
    SELECT
        repo.id AS id,
        repo.name AS name,
        repo.url as url,
        ROW_NUMBER() OVER(PARTITION BY repo.id ORDER BY created_at DESC) AS rnum
    FROM {{ ref("raw_gh_events") }}
    WHERE repo.id IS NOT NULL
        AND repo.id <> 'None'
    {% if is_incremental() %}
        AND year = EXTRACT(YEAR FROM CURRENT_DATE - 2)
        AND month = EXTRACT(MONTH FROM CURRENT_DATE - 2)
        AND day = EXTRACT(DAY FROM CURRENT_DATE - 2)
        AND CAST(created_at AS DATE) = CURRENT_DATE - 2
    {% else %}
        AND year >= 2024
    {% endif %}
)

SELECT
    {{
        dbt_utils.generate_surrogate_key([
            'id',
            'name',
        ])
    }} as surrogate_id,
    id,
    name,
    url
FROM cte_repo
WHERE rnum = 1

