{{
  config(
    materialized='table'
  )
}}

WITH action_time AS (
  SELECT
    {%- for i in range(50) %}
      SUM(CASE WHEN ACTION_{{ i }}_Action_category = 'MGT' THEN ACTION{{ i }}time_duration_sec_ ELSE 0 END) AS MGT_time_spent_{{ i }},
      SUM(CASE WHEN ACTION_{{ i }}_Action_category = 'CONTENT' THEN ACTION{{ i }}time_duration_sec_ ELSE 0 END) AS CONTENT_time_spent_{{ i }},
      SUM(CASE WHEN ACTION_{{ i }}_Action_category = 'SELF MEASUREMENT' THEN ACTION{{ i }}time_duration_sec_ ELSE 0 END) AS SELF_MEASUREMENT_time_spent_{{ i }},
      SUM(CASE WHEN ACTION_{{ i }}_Action_category = 'DIRECT CONTACT' THEN ACTION{{ i }}time_duration_sec_ ELSE 0 END) AS DIRECT_CONTACT_time_spent_{{ i }}
      {%- if not loop.last %},{% endif %}
    {%- endfor %}
  FROM
    {{ ref('src_visit_logs') }}
)

SELECT * FROM action_time