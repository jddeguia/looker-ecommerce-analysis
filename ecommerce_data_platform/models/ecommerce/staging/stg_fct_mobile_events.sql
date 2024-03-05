{{
    config(
        materialized='incremental',
        unique_key=['event_id','session_id'],
        cluster_by = ["event_type"],
        incremental_strategy = 'merge',
        partition_by={
            "field": "created_at",
            "data_type": "timestamp",
            "granularity": "day"
        } 
    )
}}

WITH events_data AS (
    SELECT * EXCEPT(id),
        id AS event_id
    FROM {{ source('ecommerce-data-platform', 'events_raw')}}
    WHERE user_id IS NOT NULL
)

SELECT * FROM events_data

 