{{
  config(
    materialized='table'
  )
}}

SELECT s.*, p.total_time_spent
FROM {{ ref('stg_feature_clicks') }}AS s
JOIN {{ ref('stg_feature_time_spent') }} AS p
USING (category)