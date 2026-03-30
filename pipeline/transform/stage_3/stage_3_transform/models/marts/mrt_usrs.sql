{{ config(materialized = "incremental",
            unique_key = ["actor_id", "actor_activity_count"]) }}

WITH actor_activity AS (
    SELECT
        act.id as actor_id,
        act.name AS actor_name,
        COUNT(*) AS actor_activity_count
    FROM {{ ref("int_dim_actor") }} act
    INNER JOIN {{ ref("int_fct_gh_events") }} fct
        ON act.id = fct.actor_id

    {% if is_incremental() %}
    WHERE fct.created_date = current_date - INTERVAL '2' DAY
    {% endif %}

    GROUP BY 1

)

SELECT actor_id, actor_name, actor_activity_count FROM actor_activity
ORDER BY 2 DESC

