
SELECT id
FROM `github-events-analysis`.`gh_events`.`int_dim_org`
GROUP BY id
HAVING COUNT(*) > 1
