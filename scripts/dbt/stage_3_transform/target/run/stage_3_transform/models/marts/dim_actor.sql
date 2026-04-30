
  
    

    create or replace table `github-dev-analytics`.`gh_archives`.`dim_actor`
      
    
    

    
    OPTIONS()
    as (
      

SELECT * from `github-dev-analytics`.`gh_archives`.`int_dim_actor`
    );
  