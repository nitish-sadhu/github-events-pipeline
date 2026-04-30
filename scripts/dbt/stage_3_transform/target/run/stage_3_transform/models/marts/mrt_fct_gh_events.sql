

  create or replace view `github-dev-analytics`.`gh_archives`.`mrt_fct_gh_events`
  OPTIONS()
  as 

SELECT
    surrogate_id,
    id,
    type,
    public,
    created_at,
    created_date
FROM `github-dev-analytics`.`gh_archives`.`int_fct_gh_events`;

