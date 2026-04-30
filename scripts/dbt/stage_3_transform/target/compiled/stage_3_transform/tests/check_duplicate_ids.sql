SELECT id, COUNT(*) FROM `github-events-analytics`.`gh_events`.`raw_gh_events`
GROUP BY id
HAVING COUNT(*) > 1