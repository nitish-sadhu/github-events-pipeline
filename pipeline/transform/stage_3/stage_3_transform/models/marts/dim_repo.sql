{{ config(materialized = "incremental",
            unique_key = "surrogate_id") }}

SELECT * FROM {{ref('int_dim_repo')}}