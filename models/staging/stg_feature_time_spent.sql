{{
  config(
    materialized='table'
  )
}}

WITH feature_time AS (
  SELECT
    {%- for i in range(50) %}
      SUM(CASE WHEN ACTION_{{ i }}_Action_category = 'MGT' THEN ACTION{{ i }}time_duration_sec_ ELSE 0 END) AS MGT_time_spent_{{ i }},
      SUM(CASE WHEN ACTION_{{ i }}_Action_category = 'CONTENT' THEN ACTION{{ i }}time_duration_sec_ ELSE 0 END) AS CONTENT_time_spent_{{ i }},
      SUM(CASE WHEN ACTION_{{ i }}_Action_category = 'SELF MEASUREMENT' THEN ACTION{{ i }}time_duration_sec_ ELSE 0 END) AS SELF_MEASUREMENT_time_spent_{{ i }},
      SUM(CASE WHEN ACTION_{{ i }}_Action_category = 'DIRECT CONTACT' THEN ACTION{{ i }}time_duration_sec_ ELSE 0 END) AS DIRECT_CONTACT_time_spent_{{ i }},
      SUM(CASE WHEN ACTION_{{ i }}_Action_category = 'ADMIN' THEN ACTION{{ i }}time_duration_sec_ ELSE 0 END) AS ADMIN_time_spent_{{ i }}
      {%- if not loop.last %},{% endif %}
    {%- endfor %}
  FROM
    {{ ref('src_visit_logs') }}
),

pivot_table AS (
  SELECT 'MGT' AS category, {%- for i in range(50) %} MGT_time_spent_{{ i }} AS time_spent {%- if not loop.last %} FROM feature_time UNION ALL SELECT 'MGT', {% endif %}{% endfor %} FROM feature_time
  UNION ALL
  SELECT 'CONTENT' AS category, {%- for i in range(50) %} CONTENT_time_spent_{{ i }} AS time_spent {%- if not loop.last %} FROM feature_time UNION ALL SELECT 'CONTENT', {% endif %}{% endfor %} FROM feature_time
  UNION ALL
  SELECT 'SELF MEASUREMENT' AS category, {%- for i in range(50) %} SELF_MEASUREMENT_time_spent_{{ i }} AS time_spent {%- if not loop.last %} FROM feature_time UNION ALL SELECT 'SELF MEASUREMENT', {% endif %}{% endfor %} FROM feature_time
  UNION ALL
  SELECT 'DIRECT CONTACT' AS category, {%- for i in range(50) %} DIRECT_CONTACT_time_spent_{{ i }} AS time_spent {%- if not loop.last %} FROM feature_time UNION ALL SELECT 'DIRECT CONTACT', {% endif %}{% endfor %} FROM feature_time
  UNION ALL
  SELECT 'ADMIN' AS category, {%- for i in range(50) %} DIRECT_CONTACT_time_spent_{{ i }} AS time_spent {%- if not loop.last %} FROM feature_time UNION ALL SELECT 'ADMIN', {% endif %}{% endfor %} FROM feature_time
)

SELECT
  category,
  SUM(time_spent) AS total_time_spent
FROM
  pivot_table
GROUP BY
  category
