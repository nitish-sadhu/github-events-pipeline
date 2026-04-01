
SELECT id, COUNT(*) FROM {{ ref("raw_gh_events") }}
GROUP BY id
HAVING COUNT(*) > 1