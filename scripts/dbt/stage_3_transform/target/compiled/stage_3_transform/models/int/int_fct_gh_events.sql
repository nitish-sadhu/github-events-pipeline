

SELECT
    to_hex(md5(cast(coalesce(cast(id as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(type as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(public as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(created_at as string), '_dbt_utils_surrogate_key_null_') as string))) AS surrogate_id,
    id,
    type,
    actor.id as actor_id,
    repo.id as repo_id,
    org.id as org_id,
    public,
    created_at,
    DATE(created_at) AS created_date
FROM `github-events-analysis`.`gh_events`.`raw_gh_events`
WHERE id IS NOT NULL

    AND year = EXTRACT(YEAR FROM CURRENT_DATE - 2)
    AND month = EXTRACT(MONTH FROM CURRENT_DATE - 2)
    AND day = EXTRACT(DAY FROM CURRENT_DATE - 2)
    AND CAST(created_at AS DATE) = CURRENT_DATE - 2
