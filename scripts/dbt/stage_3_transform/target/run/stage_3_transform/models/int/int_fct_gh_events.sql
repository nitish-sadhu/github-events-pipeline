-- back compat for old kwarg name
  
  
        
            
	    
	    
            
        
    

    

    merge into `github-events-analysis`.`gh_events`.`int_fct_gh_events` as DBT_INTERNAL_DEST
        using (

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

        ) as DBT_INTERNAL_SOURCE
        on ((DBT_INTERNAL_SOURCE.id = DBT_INTERNAL_DEST.id))

    
    when matched then update set
        `surrogate_id` = DBT_INTERNAL_SOURCE.`surrogate_id`,`id` = DBT_INTERNAL_SOURCE.`id`,`type` = DBT_INTERNAL_SOURCE.`type`,`actor_id` = DBT_INTERNAL_SOURCE.`actor_id`,`repo_id` = DBT_INTERNAL_SOURCE.`repo_id`,`org_id` = DBT_INTERNAL_SOURCE.`org_id`,`public` = DBT_INTERNAL_SOURCE.`public`,`created_at` = DBT_INTERNAL_SOURCE.`created_at`,`created_date` = DBT_INTERNAL_SOURCE.`created_date`
    

    when not matched then insert
        (`surrogate_id`, `id`, `type`, `actor_id`, `repo_id`, `org_id`, `public`, `created_at`, `created_date`)
    values
        (`surrogate_id`, `id`, `type`, `actor_id`, `repo_id`, `org_id`, `public`, `created_at`, `created_date`)


    