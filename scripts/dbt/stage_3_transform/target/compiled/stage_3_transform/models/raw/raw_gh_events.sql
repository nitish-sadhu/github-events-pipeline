


SELECT
    *
FROM `github-events-analysis`.`gh_events`.`raw_gh_events_ext`
WHERE id IS NOT NULL

    AND year = EXTRACT(YEAR FROM CURRENT_DATE - 2)
    AND month = EXTRACT(MONTH FROM CURRENT_DATE - 2)
    AND day = EXTRACT(DAY FROM CURRENT_DATE - 2)
    AND DATE(created_at) = CURRENT_DATE - 2
