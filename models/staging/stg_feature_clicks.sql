{{
  config(
    materialized='table'
  )
}}

WITH click_counts AS (
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

, pivot_data AS (
  SELECT 'MGT' AS category, {%- for i in range(51) %} MGT_{{ i }} AS count {%- if not loop.last %} FROM click_counts UNION ALL SELECT 'MGT', {% endif %}{% endfor %} FROM click_counts
  UNION ALL
  SELECT 'CONTENT' AS category, {%- for i in range(51) %} CONTENT_{{ i }} AS count {%- if not loop.last %} FROM click_counts UNION ALL SELECT 'CONTENT', {% endif %}{% endfor %} FROM click_counts
  UNION ALL
  SELECT 'ADMIN' AS category, {%- for i in range(51) %} ADMIN_{{ i }} AS count {%- if not loop.last %} FROM click_counts UNION ALL SELECT 'ADMIN', {% endif %}{% endfor %} FROM click_counts
  UNION ALL
  SELECT 'SELF MEASUREMENT' AS category, {%- for i in range(51) %} SELF_MEASUREMENT_{{ i }} AS count {%- if not loop.last %} FROM click_counts UNION ALL SELECT 'SELF MEASUREMENT', {% endif %}{% endfor %} FROM click_counts
  UNION ALL
  SELECT 'DIRECT CONTACT' AS category, {%- for i in range(51) %} DIRECT_CONTACT_{{ i }} AS count {%- if not loop.last %} FROM click_counts UNION ALL SELECT 'DIRECT CONTACT', {% endif %}{% endfor %} FROM click_counts
)

SELECT
  category,
  SUM(count) AS total_clicks
FROM
  pivot_data
GROUP BY
  category
