{{
    config(
      materialized='table'
    )
}}

WITH users AS (
    SELECT * EXCEPT(id),
        id AS user_id
    FROM {{ source('ecommerce-data-platform', 'users_raw')}}
)

SELECT * FROM users
 