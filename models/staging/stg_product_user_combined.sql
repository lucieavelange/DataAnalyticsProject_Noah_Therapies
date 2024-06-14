SELECT *
FROM {{ ref('src_visit_logs') }}
LEFT JOIN {{ ref('src_user_data')}}
USING (userId)