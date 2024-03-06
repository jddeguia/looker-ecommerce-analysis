{{
    config(
      materialized='table'
    )
}}

WITH network_summary_metrics AS (
    SELECT
        DATE(order_created_at) AS order_created_date,
        distribution_center_name,
        country AS destination_country,
        COUNTIF(shipment_status = 'Shipped') AS total_shipped_products,
        COUNTIF(shipment_status = 'Returned') AS total_returned_products,
        COUNTIF(shipment_status = 'Complete') AS total_delivered_products,
        SAFE_DIVIDE(COUNTIF(shipment_status = 'Complete'), COUNT(*)) AS completion_rate,
        SAFE_DIVIDE(COUNTIF(shipment_status = 'Returned'), COUNT(*)) AS return_rate,
        SAFE_DIVIDE(COUNTIF(shipment_status = 'Shipped'), COUNT(*)) AS pending_delivered_products_rate,
        SAFE_DIVIDE(COUNTIF(passed_delivery_performance = TRUE), COUNTIF(shipment_status = 'Complete')) AS sla_delivery_performance_rate,
        SAFE_DIVIDE(COUNTIF(passed_return_performance = TRUE), COUNTIF(shipment_status = 'Returned')) AS sla_return_performance_rate
    FROM {{ ref('mart_fct_active_orders_info')}}
    GROUP BY order_created_date, distribution_center_name, country
)

SELECT * FROM network_summary_metrics