-- back compat for old kwarg name
  
  
        
            
	    
	    
            
        
    

    

    merge into `github-events-analysis`.`gh_events`.`int_dim_actor` as DBT_INTERNAL_DEST
        using (

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
        ) as DBT_INTERNAL_SOURCE
        on ((DBT_INTERNAL_SOURCE.id = DBT_INTERNAL_DEST.id))

    
    when matched then update set
        `surrogate_id` = DBT_INTERNAL_SOURCE.`surrogate_id`,`id` = DBT_INTERNAL_SOURCE.`id`,`login` = DBT_INTERNAL_SOURCE.`login`,`url` = DBT_INTERNAL_SOURCE.`url`
    

    when not matched then insert
        (`surrogate_id`, `id`, `login`, `url`)
    values
        (`surrogate_id`, `id`, `login`, `url`)


    