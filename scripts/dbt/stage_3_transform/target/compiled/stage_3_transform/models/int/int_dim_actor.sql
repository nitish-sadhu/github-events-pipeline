

--- SELECT ONLY THE LATEST LOGIN, URL FOR THE ACTOR (TYPE - 1 TABLE)
WITH cte_actor AS (
    SELECT
        actor.id AS id,
        actor.login AS login,
        actor.url AS url,
        ROW_NUMBER() OVER(PARTITION BY actor.id ORDER BY created_at DESC) AS rnum
    FROM `github-events-analysis`.`gh_events`.`raw_gh_events`
    WHERE actor.id IS NOT NULL
        AND actor.id <> 'None'
    
        AND year = EXTRACT(YEAR FROM CURRENT_DATE - 2)
        AND month = EXTRACT(MONTH FROM CURRENT_DATE - 2)
        AND day = EXTRACT(DAY FROM CURRENT_DATE - 2)
        AND CAST(created_at AS DATE) = CURRENT_DATE - 2
    
)

SELECT
    to_hex(md5(cast(coalesce(cast(id as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(login as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(url as string), '_dbt_utils_surrogate_key_null_') as string))) as surrogate_id,
    id,
    login,
    url
FROM cte_actor
WHERE rnum = 1