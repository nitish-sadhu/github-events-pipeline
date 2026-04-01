
{{ config(materialized = "table") }}

WITH actor_activity AS (
    SELECT
        act.id as actor_id,
        act.login AS actor_login,
        COUNT(*) AS actor_activity_count
    FROM {{ ref("int_dim_actor") }} act
    INNER JOIN {{ ref("int_fct_gh_events") }} fct
        ON act.id = fct.actor_id
    --WHERE fct.created_date >= current_date - INTERVAL 30 DAY
    GROUP BY 1, 2

)


SELECT actor_id, actor_login, actor_activity_count FROM actor_activity
ORDER BY 3 DESC

