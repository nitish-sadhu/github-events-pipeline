
  
    

    create or replace table `github-dev-analytics`.`gh_archives`.`mrt_dim_repo`
      
    
    

    
    OPTIONS()
    as (
      

SELECT
    surrogate_id,
    id,
    name,
    url
FROM `github-dev-analytics`.`gh_archives`.`int_dim_repo`
    );
  