{{ config(materialized = "incremental",
            unique_key = "surrogate_id") }}

SELECT * from {{ref('int_dim_actor')}}

