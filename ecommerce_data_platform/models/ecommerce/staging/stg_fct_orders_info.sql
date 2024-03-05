{{
    config(
        materialized='incremental',
        unique_key=['user_id','order_id'],
        cluster_by = ["status"],
        incremental_strategy = 'merge',
        partition_by={
            "field": "created_at",
            "data_type": "timestamp",
            "granularity": "day"
        } 
    )
}}

WITH orders AS (
    SELECT * EXCEPT(id),
        id AS shipment_id
    FROM {{ source('ecommerce-data-platform', 'orders_raw')}}
    WHERE created_at <= shipped_at
)

SELECT * FROM orders

 