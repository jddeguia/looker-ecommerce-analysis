{{
    config(
      materialized='table'
    )
}}

WITH inventory_items AS (
    SELECT * EXCEPT(id, product_id),
        id AS inventory_item_id,
        product_id AS product_category_id
    FROM {{ source('ecommerce-data-platform', 'inventory_items_raw')}}
)

SELECT * FROM inventory_items

 