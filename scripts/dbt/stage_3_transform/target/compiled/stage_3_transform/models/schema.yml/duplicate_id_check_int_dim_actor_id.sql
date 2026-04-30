
SELECT id
FROM `github-events-analysis`.`gh_events`.`int_dim_actor`
GROUP BY id
HAVING COUNT(*) > 1
