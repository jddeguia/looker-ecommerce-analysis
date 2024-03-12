{% set min_created_date = '2021-01-01' %}

{{
    config(
      materialized='table'
    )
}}

WITH user_engagement_info AS (
    SELECT * EXCEPT (ip_address,city,postal_code,event_id)
    FROM {{ ref('stg_fct_mobile_events') }}
    WHERE DATE(created_at) >= DATE('{{ min_created_date }}')
    {%- if is_incremental() or target.name == 'dev' %}
    AND DATE(created_at) BETWEEN DATE('{{ var("from_date") }}') AND DATE('{{ var("to_date") }}')
    {%- endif %}
),

extract_info_from_uri AS (
    SELECT *,
        REGEXP_EXTRACT(uri, r'/department/([^/]+)') AS department,
        REGEXP_EXTRACT(uri, r'/category/([^/]+)') AS category,
        REGEXP_EXTRACT(uri, r'/brand/([^/]+)') AS brand,
        REGEXP_EXTRACT(uri, r'/product/(\d+)') AS product_id
    FROM user_engagement_info
)

SELECT * EXCEPT (uri) FROM extract_info_from_uri 