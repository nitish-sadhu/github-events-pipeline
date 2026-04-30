
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
SELECT surrogate_id
FROM `github-events-analytics`.`gh_events`.`int_dim_actor`
GROUP BY surrogate_id
HAVING COUNT(*) > 1

  
  
      
    ) dbt_internal_test