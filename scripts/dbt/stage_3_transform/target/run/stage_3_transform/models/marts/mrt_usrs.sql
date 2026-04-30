
  
    

    create or replace table `github-events-analysis`.`gh_events`.`mrt_usrs`
      
    
    

    
    OPTIONS()
    as (
      

WITH actor_activity AS (
    SELECT
        act.id as actor_id,
        act.login AS actor_login,
        COUNT(*) AS actor_activity_count
    FROM `github-events-analysis`.`gh_events`.`int_dim_actor` act
    INNER JOIN `github-events-analysis`.`gh_events`.`int_fct_gh_events` fct
        ON act.id = fct.actor_id
    WHERE fct.created_date >= current_date - INTERVAL 30 DAY
    GROUP BY 1, 2

)


SELECT actor_id, actor_login, actor_activity_count FROM actor_activity
ORDER BY 3 DESC
    );
  