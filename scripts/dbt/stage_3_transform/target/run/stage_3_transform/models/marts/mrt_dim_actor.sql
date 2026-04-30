
  
    

    create or replace table `github-dev-analytics`.`gh_archives`.`mrt_dim_actor`
      
    
    

    
    OPTIONS()
    as (
      

SELECT
    surrogate_id,
    id,
    login,
    display_login,
    gravatar_id,
    url,
    avatar_url
FROM `github-dev-analytics`.`gh_archives`.`int_dim_actor`
    );
  