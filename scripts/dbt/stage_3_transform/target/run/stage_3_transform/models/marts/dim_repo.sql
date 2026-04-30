
  
    

    create or replace table `github-dev-analytics`.`gh_archives`.`dim_repo`
      
    
    

    
    OPTIONS()
    as (
      

SELECT * FROM `github-dev-analytics`.`gh_archives`.`int_dim_repo`
    );
  