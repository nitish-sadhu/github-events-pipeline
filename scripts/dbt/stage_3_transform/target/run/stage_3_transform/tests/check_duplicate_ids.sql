
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  SELECT id, COUNT(*) FROM `github-events-analytics`.`gh_events`.`raw_gh_events`
GROUP BY id
HAVING COUNT(*) > 1
  
  
      
    ) dbt_internal_test