
/*
WITH cte_org AS (
    SELECT
        to_hex(md5(cast(coalesce(cast(org.id as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(org.login as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(org.gravatar_id as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(org.url as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(org.avatar_url as string), '_dbt_utils_surrogate_key_null_') as string))) as surrogate_id,
        org.id AS id,
        org.login AS login,
        org.gravatar_id AS gravatar_id,
        org.url as url,
        org.avatar_url as avatar_url
    FROM `github-events-analysis`.`gh_events`.`raw_gh_events`
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
    FROM `github-events-analysis`.`gh_events`.`raw_gh_events`
    WHERE org.id IS NOT NULL
        AND org.id <> 'None'
    
        AND year = EXTRACT(YEAR FROM CURRENT_DATE - 2)
        AND month = EXTRACT(MONTH FROM CURRENT_DATE - 2)
        AND day = EXTRACT(DAY FROM CURRENT_DATE - 2)
        AND CAST(created_at AS DATE) = CURRENT_DATE - 2
    
)

SELECT
    to_hex(md5(cast(coalesce(cast(id as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(login as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(url as string), '_dbt_utils_surrogate_key_null_') as string))) AS surrogate_id,
    id,
    login,
    url
FROM cte_org
WHERE rnum = 1