-- back compat for old kwarg name
  
  
        
            
	    
	    
            
        
    

    

    merge into `github-events-analysis`.`gh_events`.`raw_gh_events` as DBT_INTERNAL_DEST
        using (


SELECT
    *
FROM `github-events-analysis`.`gh_events`.`raw_gh_events_ext`
WHERE id IS NOT NULL

    AND year = EXTRACT(YEAR FROM CURRENT_DATE - 2)
    AND month = EXTRACT(MONTH FROM CURRENT_DATE - 2)
    AND day = EXTRACT(DAY FROM CURRENT_DATE - 2)
    AND DATE(created_at) = CURRENT_DATE - 2

        ) as DBT_INTERNAL_SOURCE
        on ((DBT_INTERNAL_SOURCE.surrogate_id = DBT_INTERNAL_DEST.surrogate_id))

    
    when matched then update set
        `surrogate_id` = DBT_INTERNAL_SOURCE.`surrogate_id`,`id` = DBT_INTERNAL_SOURCE.`id`,`type` = DBT_INTERNAL_SOURCE.`type`,`actor` = DBT_INTERNAL_SOURCE.`actor`,`repo` = DBT_INTERNAL_SOURCE.`repo`,`public` = DBT_INTERNAL_SOURCE.`public`,`created_at` = DBT_INTERNAL_SOURCE.`created_at`,`org` = DBT_INTERNAL_SOURCE.`org`,`year` = DBT_INTERNAL_SOURCE.`year`,`month` = DBT_INTERNAL_SOURCE.`month`,`day` = DBT_INTERNAL_SOURCE.`day`
    

    when not matched then insert
        (`surrogate_id`, `id`, `type`, `actor`, `repo`, `public`, `created_at`, `org`, `year`, `month`, `day`)
    values
        (`surrogate_id`, `id`, `type`, `actor`, `repo`, `public`, `created_at`, `org`, `year`, `month`, `day`)


    