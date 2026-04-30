
SELECT surrogate_id
FROM `github-events-analytics`.`gh_events`.`int_dim_org`
GROUP BY surrogate_id
HAVING COUNT(*) > 1
