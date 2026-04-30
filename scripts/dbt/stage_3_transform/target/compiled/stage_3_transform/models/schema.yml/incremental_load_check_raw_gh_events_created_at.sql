
SELECT created_at, COUNT(*) FROM `github-events-analysis`.`gh_events`.`raw_gh_events`
WHERE CAST(created_at AS DATE) = CURRENT_DATE - 2
GROUP BY created_at
HAVING COUNT(*) = 0
