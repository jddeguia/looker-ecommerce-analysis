{{
    config(
      materialized='table'
    )
}}

WITH user_engagement_metrics AS (
    SELECT
        DATE(created_at) AS session_created_date,
        traffic_source,
        state,
        category,
        department,
        brand,
        COUNT(session_id) AS total_sessions,
        COUNTIF(event_type = 'home') AS total_home_impression,
        COUNTIF(event_type = 'product')  AS total_product_impression,
        COUNTIF(event_type = 'cancel')  AS total_cancelled_transaction,
        COUNTIF(event_type = 'purchase') AS total_purchased_transaction,
        COUNTIF(event_type = 'cart')  AS total_add_to_cart,
        COUNTIF(event_type = 'department') AS total_department_impression
    FROM {{ ref('mart_fct_cleaned_mobile_events')}}
    GROUP BY 
        session_created_date, 
        traffic_source, 
        state, 
        category, 
        department, 
        brand
)

SELECT * FROM user_engagement_metrics