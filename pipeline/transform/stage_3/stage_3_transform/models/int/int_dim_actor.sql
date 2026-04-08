{{ config(materialized = "incremental",
            unique_key = "surrogate_id") }}

/*
WITH cte_actor AS (
    SELECT
        {{
            dbt_utils.generate_surrogate_key([
                'actor.id',
                'actor.login',
                'actor.display_login',
                'actor.gravatar_id',
                'actor.url',
                'actor.avatar_url'
        ])
        }} as surrogate_id,
        actor.id AS id,
        actor.login AS login,
        actor.display_login AS display_login,
        actor.gravatar_id AS gravatar_id,
        actor.url as url,
        actor.avatar_url as avatar_url
    FROM {{ ref("raw_gh_events") }}
    WHERE actor.id IS NOT NULL
)

SELECT DISTINCT * FROM cte_actor
*/

--- SELECT ONLY THE LATEST LOGIN, URL FOR THE ACTOR (TYPE - 1 TABLE)
WITH cte_actor AS (
    SELECT
        actor.id AS id,
        actor.login AS login,
        actor.url AS url,
        ROW_NUMBER() OVER(PARTITION BY actor.id ORDER BY created_at DESC) AS rnum
    FROM {{ ref("raw_gh_events") }}
    WHERE actor.id IS NOT NULL
        and actor.id <> 'None'
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




