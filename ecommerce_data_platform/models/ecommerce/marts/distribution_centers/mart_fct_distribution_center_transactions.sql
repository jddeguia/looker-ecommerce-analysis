{% set min_created_date = '2021-01-01' %}

{{
    config(
      materialized='table'
    )
}}

WITH item_transaction_info AS (
    SELECT * 
    FROM {{ ref('stg_fct_inventory_items') }}
    WHERE DATE(created_at) >= DATE('{{ min_created_date }}')
    {%- if is_incremental() or target.name == 'dev' %}
    AND DATE(created_at) BETWEEN DATE('{{ var("from_date") }}') AND DATE('{{ var("to_date") }}')
    {%- endif %}
),

distribution_center_info AS (
    SELECT *
    FROM {{ ref('stg_dim_distribution_centers')}}
),

summary AS (
    SELECT
        inventory_item_id,
        product_name,
        product_brand,
        product_category,
        product_retail_price,
        product_department,
        product_sku,
        created_at ,
        sold_at,
        cost,
        d.name AS distribution_center_name,
        latitude,
        longitude
    FROM item_transaction_info t
    LEFT JOIN distribution_center_info d on t.product_distribution_center_id = distribution_center_id
)

SELECT * FROM summary