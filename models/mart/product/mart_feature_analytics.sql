WITH feature AS (
    SELECT
        CASE
            WHEN basicCondition IS NOT NULL THEN basicCondition
            ELSE 'Unknown'
        END AS basicCondition_category,
        CASE
            WHEN age BETWEEN 18 AND 24 THEN '18-24'
            WHEN age BETWEEN 25 AND 34 THEN '25-34'
            WHEN age BETWEEN 35 AND 44 THEN '35-44'
            WHEN age BETWEEN 45 AND 54 THEN '45-54'
            WHEN age > 54 THEN '55+'
            ELSE 'Unknown'
        END AS age_category,
        {%- for i in range(50) %}
            SUM(CASE WHEN ACTION_{{ i }}_Action_category = 'MGT' THEN ACTION{{ i }}time_duration_sec_ ELSE 0 END) AS MGT_time_spent_{{ i }},
            SUM(CASE WHEN ACTION_{{ i }}_Action_category = 'CONTENT' THEN ACTION{{ i }}time_duration_sec_ ELSE 0 END) AS CONTENT_time_spent_{{ i }},
            SUM(CASE WHEN ACTION_{{ i }}_Action_category = 'SELF MEASUREMENT' THEN ACTION{{ i }}time_duration_sec_ ELSE 0 END) AS SELF_MEASUREMENT_time_spent_{{ i }},
            SUM(CASE WHEN ACTION_{{ i }}_Action_category = 'DIRECT CONTACT' THEN ACTION{{ i }}time_duration_sec_ ELSE 0 END) AS DIRECT_CONTACT_time_spent_{{ i }},
            SUM(CASE WHEN ACTION_{{ i }}_Action_category = 'ADMIN' THEN ACTION{{ i }}time_duration_sec_ ELSE 0 END) AS ADMIN_time_spent_{{ i }},
            SUM(CASE WHEN ACTION_{{ i }}_Action_category IS NULL THEN ACTION{{ i }}time_duration_sec_ ELSE 0 END) AS ACTION_TYPE_time_spent_{{ i }},
            SUM(CASE WHEN ACTION_{{ i }}_Action_category = 'MGT' THEN 1 ELSE 0 END) AS MGT_{{ i }},
            SUM(CASE WHEN ACTION_{{ i }}_Action_category = 'CONTENT' THEN 1 ELSE 0 END) AS CONTENT_{{ i }},
            SUM(CASE WHEN ACTION_{{ i }}_Action_category = 'ADMIN' THEN 1 ELSE 0 END) AS ADMIN_{{ i }},
            SUM(CASE WHEN ACTION_{{ i }}_Action_category = 'SELF MEASUREMENT' THEN 1 ELSE 0 END) AS SELF_MEASUREMENT_{{ i }},
            SUM(CASE WHEN ACTION_{{ i }}_Action_category = 'DIRECT CONTACT' THEN 1 ELSE 0 END) AS DIRECT_CONTACT_{{ i }},
            SUM(CASE WHEN ACTION_{{ i }}_Action_category IS NULL THEN 1 ELSE 0 END) AS ACTION_TYPE_{{ i }}
            {%- if not loop.last %},{% endif %}
        {%- endfor %}
    FROM 
        {{ ref('stg_product_user_combined') }}
    GROUP BY 
        basicCondition_category, age_category
),

summed_feature AS (
    SELECT
        basicCondition_category,
        age_category,
        SUM({%- for i in range(50) %} MGT_time_spent_{{ i }} {%- if not loop.last %} + {% endif %} {%- endfor %}) AS total_time_spent_mgt,
        SUM({%- for i in range(50) %} CONTENT_time_spent_{{ i }} {%- if not loop.last %} + {% endif %} {%- endfor %}) AS total_time_spent_content,
        SUM({%- for i in range(50) %} ADMIN_time_spent_{{ i }} {%- if not loop.last %} + {% endif %} {%- endfor %}) AS total_time_spent_admin,
        SUM({%- for i in range(50) %} SELF_MEASUREMENT_time_spent_{{ i }} {%- if not loop.last %} + {% endif %} {%- endfor %}) AS total_time_spent_self_measurement,
        SUM({%- for i in range(50) %} DIRECT_CONTACT_time_spent_{{ i }} {%- if not loop.last %} + {% endif %} {%- endfor %}) AS total_time_spent_direct_contact,
        SUM({%- for i in range(50) %} ACTION_TYPE_time_spent_{{ i }} {%- if not loop.last %} + {% endif %} {%- endfor %}) AS total_time_spent_action_type,
        SUM({%- for i in range(50) %} MGT_{{ i }} {%- if not loop.last %} + {% endif %} {%- endfor %}) AS count_feature_mgt,
        SUM({%- for i in range(50) %} CONTENT_{{ i }} {%- if not loop.last %} + {% endif %} {%- endfor %}) AS count_feature_content,
        SUM({%- for i in range(50) %} ADMIN_{{ i }} {%- if not loop.last %} + {% endif %} {%- endfor %}) AS count_feature_admin,
        SUM({%- for i in range(50) %} SELF_MEASUREMENT_{{ i }} {%- if not loop.last %} + {% endif %} {%- endfor %}) AS count_feature_self_measurement,
        SUM({%- for i in range(50) %} DIRECT_CONTACT_{{ i }} {%- if not loop.last %} + {% endif %} {%- endfor %}) AS count_feature_direct_contact,
        SUM({%- for i in range(50) %} ACTION_TYPE_{{ i }} {%- if not loop.last %} + {% endif %} {%- endfor %}) AS count_feature_action_type
    FROM
        feature
    GROUP BY
        basicCondition_category, age_category
),

pivot_table AS (
    SELECT basicCondition_category, age_category, 'MGT' AS category, total_time_spent_mgt AS time_spent, count_feature_mgt AS count_feature FROM summed_feature
    UNION ALL
    SELECT basicCondition_category, age_category, 'CONTENT' AS category, total_time_spent_content AS time_spent, count_feature_content AS count_feature FROM summed_feature
    UNION ALL
    SELECT basicCondition_category, age_category, 'SELF MEASUREMENT' AS category, total_time_spent_self_measurement AS time_spent, count_feature_self_measurement AS count_feature FROM summed_feature
    UNION ALL
    SELECT basicCondition_category, age_category, 'DIRECT CONTACT' AS category, total_time_spent_direct_contact AS time_spent, count_feature_direct_contact AS count_feature FROM summed_feature
    UNION ALL
    SELECT basicCondition_category, age_category, 'ADMIN' AS category, total_time_spent_admin AS time_spent, count_feature_admin AS count_feature FROM summed_feature
    UNION ALL
    SELECT basicCondition_category, age_category, 'ACTION_TYPE' AS category, total_time_spent_action_type AS time_spent, count_feature_action_type AS count_feature FROM summed_feature
)

SELECT
    basicCondition_category,
    age_category,
    category,
    time_spent,
    count_feature
FROM
    pivot_table