{{
    config(
      materialized='table'
    )
}}

WITH products AS (
    SELECT * EXCEPT(id),
        id AS product_id
    FROM {{ source('ecommerce-data-platform', 'products_raw')}}
)

SELECT * FROM products