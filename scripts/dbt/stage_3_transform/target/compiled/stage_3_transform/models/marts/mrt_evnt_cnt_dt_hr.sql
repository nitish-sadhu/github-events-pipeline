

SELECT
    created_date,
    EXTRACT(HOUR FROM created_at) as created_hour,
    type as event_type,
    COUNT(*) AS event_counts
FROM `github-events-analysis`.`gh_events`.`int_fct_gh_events`


WHERE created_date = CURRENT_DATE - 2


GROUP BY 1, 2, 3