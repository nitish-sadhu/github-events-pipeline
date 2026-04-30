
SELECT surrogate_id
FROM `github-events-analytics`.`gh_events`.`int_dim_actor`
GROUP BY surrogate_id
HAVING COUNT(*) > 1
