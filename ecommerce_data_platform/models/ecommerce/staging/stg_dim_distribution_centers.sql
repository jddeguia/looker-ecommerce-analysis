{{
    config(
      materialized='table'
    )
}}

WITH distribution_centers AS (
    SELECT * EXCEPT(id),
        id AS distribution_center_id
    FROM {{ source('ecommerce-data-platform', 'distribution_centers_raw')}}
)

SELECT * FROM distribution_centers

 