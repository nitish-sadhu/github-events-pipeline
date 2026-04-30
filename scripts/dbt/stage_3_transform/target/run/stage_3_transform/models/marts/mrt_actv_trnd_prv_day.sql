
  
    

    create or replace table `github-events-analysis`.`gh_events`.`mrt_actv_trnd_prv_day`
      
    
    

    
    OPTIONS()
    as (
      

WITH counts_per_day AS (
    SELECT
        created_date,
        COUNT(*) AS counts
    FROM `github-events-analysis`.`gh_events`.`int_fct_gh_events`
    GROUP BY 1
)

SELECT
    created_date,
    counts
FROM counts_per_day
ORDER BY 1 DESC
    );
  