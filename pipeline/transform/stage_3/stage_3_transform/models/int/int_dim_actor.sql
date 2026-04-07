{{ config(materialized = "table") }}

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

