{{ config(materialized = "incremental",
            unique_key = "surrogate_id") }}

SELECT
    *
FROM {{ ref("int_fct_gh_events") }}

