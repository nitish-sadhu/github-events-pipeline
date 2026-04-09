{{ config(materialized = "incremental",
            unique_key = "id") }}

--- SELECT ONLY THE LATEST LOGIN, URL FOR THE ACTOR (TYPE - 1 TABLE)
WITH cte_actor AS (
    SELECT
        actor.id AS id,
        actor.login AS login,
        actor.url AS url,
        ROW_NUMBER() OVER(PARTITION BY actor.id ORDER BY created_at DESC) AS rnum
    FROM {{ ref("raw_gh_events") }}
    WHERE actor.id IS NOT NULL
        AND actor.id <> 'None'
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
    }} as surrogate_id,
    id,
    login,
    url
FROM cte_actor
WHERE rnum = 1




