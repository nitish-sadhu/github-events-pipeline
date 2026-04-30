
SELECT surrogate_id
FROM `github-events-analysis`.`gh_events`.`raw_gh_events`
GROUP BY surrogate_id
HAVING COUNT(*) > 1
