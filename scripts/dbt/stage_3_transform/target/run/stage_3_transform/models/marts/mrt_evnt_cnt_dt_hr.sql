-- back compat for old kwarg name
  
  
        
            
                
                
            
                
                
            
                
                
            
        
    

    

    merge into `github-events-analysis`.`gh_events`.`mrt_evnt_cnt_dt_hr` as DBT_INTERNAL_DEST
        using (

SELECT
    created_date,
    EXTRACT(HOUR FROM created_at) as created_hour,
    type as event_type,
    COUNT(*) AS event_counts
FROM `github-events-analysis`.`gh_events`.`int_fct_gh_events`


WHERE created_date = CURRENT_DATE - 2


GROUP BY 1, 2, 3
        ) as DBT_INTERNAL_SOURCE
        on (
                    DBT_INTERNAL_SOURCE.created_date = DBT_INTERNAL_DEST.created_date
                ) and (
                    DBT_INTERNAL_SOURCE.created_hour = DBT_INTERNAL_DEST.created_hour
                ) and (
                    DBT_INTERNAL_SOURCE.event_type = DBT_INTERNAL_DEST.event_type
                )

    
    when matched then update set
        `created_date` = DBT_INTERNAL_SOURCE.`created_date`,`created_hour` = DBT_INTERNAL_SOURCE.`created_hour`,`event_type` = DBT_INTERNAL_SOURCE.`event_type`,`event_counts` = DBT_INTERNAL_SOURCE.`event_counts`
    

    when not matched then insert
        (`created_date`, `created_hour`, `event_type`, `event_counts`)
    values
        (`created_date`, `created_hour`, `event_type`, `event_counts`)


    