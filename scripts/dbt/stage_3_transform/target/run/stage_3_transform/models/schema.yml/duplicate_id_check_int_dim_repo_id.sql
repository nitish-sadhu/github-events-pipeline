
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
SELECT id
FROM `github-events-analysis`.`gh_events`.`int_dim_repo`
GROUP BY id
HAVING COUNT(*) > 1

  
  
      
    ) dbt_internal_test