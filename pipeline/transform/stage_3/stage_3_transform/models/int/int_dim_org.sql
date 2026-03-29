{{ config(materialized = "incremental") }}

SELECT
    {{
        dbt_utils.generate_surrogate_key([
            'org.id',
            'org.login',
            'org.display_login',
            'org.url',
            'org.avatar_url'
    ])
    }} as surrogate_id,
    org.id AS id,
    org.login AS login,
    org.gravatar_id AS gravatar_id,
    org.url as url,
    org.avatar_url as avatar_url
FROM {{ ref("raw_gh_events") }}
WHERE org.id IS NOT NULL


