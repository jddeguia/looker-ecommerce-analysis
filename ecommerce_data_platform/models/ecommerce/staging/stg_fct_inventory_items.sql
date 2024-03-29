{% set min_created_date = '2021-01-01' %}

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
    
    WHERE DATE(created_at) >= DATE('{{ min_created_date }}')
    {%- if is_incremental() or target.name == 'dev' %}
    AND DATE(created_at) BETWEEN DATE('{{ var("from_date") }}') AND DATE('{{ var("to_date") }}')
    {%- endif %}
)

SELECT * FROM inventory_items

 