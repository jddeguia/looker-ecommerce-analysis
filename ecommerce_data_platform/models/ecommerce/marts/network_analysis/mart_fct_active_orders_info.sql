{% set min_created_date = '2021-01-01' %}

{{
    config(
        materialized='incremental',
        unique_key=['order_id','user_id'],
        cluster_by = ["shipment_status"],
        incremental_strategy = 'merge',
        partition_by={
            "field": "order_created_at",
            "data_type": "timestamp",
            "granularity": "day"
        } 
    )
}}

WITH active_shipments AS (
    SELECT * 
    FROM {{ ref('stg_fct_orders_info') }}
    WHERE DATE(created_at) >= DATE('{{ min_created_date }}')
    {%- if is_incremental() or target.name == 'dev' %}
    AND DATE(created_at) BETWEEN DATE('{{ var("from_date") }}') AND DATE('{{ var("to_date") }}')
    {%- endif %}
),

parcel_destination_info AS (
    SELECT
        user_id,
        country,
        latitude,
        longitude
    FROM {{ ref('stg_dim_users_info')}}
),

product_info AS (
    SELECT
        product_id,
        cost,
        category,
        p.name AS product_name,
        brand,
        retail_price,
        department,
        sku,
        d.name AS distribution_center_name  
    FROM {{ ref('stg_dim_products_info')}} p
    INNER JOIN {{ ref('stg_dim_distribution_centers')}} d USING (distribution_center_id)
),

add_continent AS (
    SELECT 
        order_id,
        user_id,
        country,
        CASE 
            WHEN country IN ('Japan','South Korea','China') THEN 'Asia'
            WHEN country IN ('España','Spain', 'France', 'Germany', 'Poland', 'United Kingdom', 'Belgium') THEN 'Europe'
            WHEN country IN ('Brasil', 'Colombia') THEN 'South America'
            WHEN country IN ('United States') THEN 'North America'
            WHEN country IN ('Australia') THEN 'Australia'
        END AS continent,
        latitude,
        longitude
        product_id,
        inventory_item_id,
        product_name,
        category AS product_category,
        brand AS product_brand,
        retail_price,
        department AS product_department,
        sku AS product_sku,
        distribution_center_name,
        status AS shipment_status, 
        created_at AS order_created_at,
        shipped_at,
        delivered_at,
        returned_at,
        shipment_id
    FROM active_shipments
    INNER JOIN parcel_destination_info USING (user_id)
    INNER JOIN product_info USING (product_id)
    {%- if is_incremental() or target.name == 'dev' %}
    WHERE DATE(created_at) BETWEEN DATE('{{ var("from_date") }}') AND DATE('{{ var("to_date") }}')
    {%- endif %}
),

add_sla AS (
    SELECT *,
        CASE 
            {% for continent in ['Asia', 'Europe', 'South America', 'North America', 'Australia'] %}
                {% set interval_days = 
                    4 if continent == 'Asia' else 
                    3 if continent == 'Europe' else 
                    3 if continent == 'South America' else
                    2 if continent == 'North America' else
                    3 if continent == 'Australia' else
                    0 
                %}
                WHEN continent = '{{ continent }}' THEN TIMESTAMP_ADD(order_created_at, INTERVAL {{ interval_days }} DAY)
            {% endfor %}
            ELSE NULL
        END AS delivery_sla,
        CASE 
            {% for continent in ['Asia', 'Europe', 'South America', 'North America', 'Australia'] %}
                {% set interval_days = 
                    7 if continent == 'Asia' else 
                    7 if continent == 'Europe' else 
                    7 if continent == 'South America' else
                    5 if continent == 'North America' else
                    5 if continent == 'Australia' else
                    0 
                %}
                WHEN continent = '{{ continent }}' THEN TIMESTAMP_ADD(order_created_at, INTERVAL {{ interval_days }} DAY)
            {% endfor %}
            ELSE NULL
        END AS return_sla
    FROM add_continent
),

add_speed_metric AS (
    SELECT *,
        delivered_at <= delivery_sla AS passed_delivery_performance,
        returned_at <= return_sla AS passed_return_performance
    FROM add_sla
)

SELECT * FROM add_speed_metric
