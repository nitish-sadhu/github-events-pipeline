{{ config(materialized = "incremental",
            unique_key = "id") }}
/*
WITH cte_org AS (
    SELECT
        {{
            dbt_utils.generate_surrogate_key([
                'org.id',
                'org.login',
                'org.gravatar_id',
                'org.url',
                'org.avatar_url'
        ])
        }} as surrogate_id,
        org.id AS id,
        org.login AS login,
        org.gravatar_id AS gravatar_id,
        org.url as url,
        org.avatar_url as avatar_url
    FROM {{ ref("raw_gh_events") }}
    WHERE org.id IS NOT NULL
)

SELECT DISTINCT * FROM cte_org
*/

WITH cte_org AS (
    SELECT
        org.id AS id,
        org.login AS login,
        org.url AS url,
        ROW_NUMBER() OVER(PARTITION BY org.id ORDER BY created_at DESC) AS rnum
    FROM {{ ref("raw_gh_events") }}
    WHERE org.id IS NOT NULL
        AND org.id <> 'None'
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
            'login',
            'url'
        ])
    }} AS surrogate_id,
    id,
    login,
    url
FROM cte_org
WHERE rnum = 1


