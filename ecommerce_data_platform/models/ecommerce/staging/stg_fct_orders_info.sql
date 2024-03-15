{% set min_created_date = '2021-01-01' %}

{{
    config(
      materialized='table'
    )
}}

WITH orders AS (
    SELECT * EXCEPT(id),
        id AS shipment_id
    FROM {{ source('ecommerce-data-platform', 'orders_raw')}}
    WHERE created_at <= shipped_at
    AND DATE(created_at) >= DATE('{{ min_created_date }}')
    {%- if is_incremental() or target.name == 'dev' %}
    AND DATE(created_at) BETWEEN DATE('{{ var("from_date") }}') AND DATE('{{ var("to_date") }}')
    {%- endif %}
)

SELECT * FROM orders

 