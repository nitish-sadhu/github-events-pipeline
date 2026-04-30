-- back compat for old kwarg name
  
  
        
            
	    
	    
            
        
    

    

    merge into `github-events-analysis`.`gh_events`.`int_dim_repo` as DBT_INTERNAL_DEST
        using (
/*
SELECT
    DISTINCT
    to_hex(md5(cast(coalesce(cast(repo.id as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(repo.name as string), '_dbt_utils_surrogate_key_null_') as string))) as surrogate_id,
    repo.id AS id,
    repo.name AS name,
    repo.url as url
FROM `github-events-analysis`.`gh_events`.`raw_gh_events`
WHERE repo.id IS NOT NULL AND
      repo.id <> 'None'
        AND CAST(created_at AS DATE) = (SELECT MAX(CAST(created_at AS DATE)) FROM )
*/

---SELECT ONLY THE LATEST NAME AND URL FOR THE REPO (TYPE - 1 TABLE).

WITH cte_repo AS (
    SELECT
        repo.id AS id,
        repo.name AS name,
        repo.url as url,
        ROW_NUMBER() OVER(PARTITION BY repo.id ORDER BY created_at DESC) AS rnum
    FROM `github-events-analysis`.`gh_events`.`raw_gh_events`
    WHERE repo.id IS NOT NULL
        AND repo.id <> 'None'
    
        AND year = EXTRACT(YEAR FROM CURRENT_DATE - 2)
        AND month = EXTRACT(MONTH FROM CURRENT_DATE - 2)
        AND day = EXTRACT(DAY FROM CURRENT_DATE - 2)
        AND CAST(created_at AS DATE) = CURRENT_DATE - 2
    
)

SELECT
    to_hex(md5(cast(coalesce(cast(id as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(name as string), '_dbt_utils_surrogate_key_null_') as string))) as surrogate_id,
    id,
    name,
    url
FROM cte_repo
WHERE rnum = 1
        ) as DBT_INTERNAL_SOURCE
        on ((DBT_INTERNAL_SOURCE.id = DBT_INTERNAL_DEST.id))

    
    when matched then update set
        `surrogate_id` = DBT_INTERNAL_SOURCE.`surrogate_id`,`id` = DBT_INTERNAL_SOURCE.`id`,`name` = DBT_INTERNAL_SOURCE.`name`,`url` = DBT_INTERNAL_SOURCE.`url`
    

    when not matched then insert
        (`surrogate_id`, `id`, `name`, `url`)
    values
        (`surrogate_id`, `id`, `name`, `url`)


    