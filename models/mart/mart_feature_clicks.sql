{{
  config(
    materialized='table'
  )
}}

WITH action_counts AS (
  SELECT
    {%- for i in range(51) %}
      SUM(CASE WHEN ACTION_{{ i }}_Action_category = 'MGT' THEN CAST(1 AS INT64) ELSE 0 END) AS MGT_{{ i }},
      SUM(CASE WHEN ACTION_{{ i }}_Action_category = 'CONTENT' THEN CAST(1 AS INT64) ELSE 0 END) AS CONTENT_{{ i }},
      SUM(CASE WHEN ACTION_{{ i }}_Action_category = 'ADMIN' THEN CAST(1 AS INT64) ELSE 0 END) AS ADMIN_{{ i }},
      SUM(CASE WHEN ACTION_{{ i }}_Action_category = 'SELF MEASUREMENT' THEN CAST(1 AS INT64) ELSE 0 END) AS SELF_MEASUREMENT_{{ i }},
      SUM(CASE WHEN ACTION_{{ i }}_Action_category = 'DIRECT CONTACT' THEN CAST(1 AS INT64) ELSE 0 END) AS DIRECT_CONTACT_{{ i }}
      {%- if not loop.last %},{% endif %}
    {%- endfor %}
  FROM
    {{ ref('src_visit_logs') }}
)

SELECT * FROM action_counts