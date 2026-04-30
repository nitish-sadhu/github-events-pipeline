
  
    

    create or replace table `github-dev-analytics`.`gh_archives`.`mrt_dim_org`
      
    
    

    
    OPTIONS()
    as (
      

SELECT
    surrogate_id,
    id,
    login,
    gravatar_id,
    url,
    avatar_url
FROM `github-dev-analytics`.`gh_archives`.`int_dim_org`
    );
  