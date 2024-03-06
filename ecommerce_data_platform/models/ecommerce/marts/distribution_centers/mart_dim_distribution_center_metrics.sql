{{
    config(
      materialized='table'
    )
}}

WITH distribution_center_metrics AS (
    SELECT
        DATE(created_at) AS created_date,
        distribution_center_name,
        COUNTIF(sold_at IS NOT NULL) AS total_sold_products,
        COUNT(*) AS total_active_products,
        SAFE_DIVIDE(COUNTIF(sold_at IS NOT NULL), COUNT(*)) AS selling_rate,
        SUM(product_retail_price) AS total_sales,
        SUM(cost) AS total_cost
    FROM {{ ref('mart_fct_distribution_center_transactions')}}
    GROUP BY created_date, distribution_center_name
),

summary AS (
    SELECT *,
        SAFE_DIVIDE((total_sales - total_cost), total_sales) AS margin
    FROM distribution_center_metrics
)

SELECT * FROM summary