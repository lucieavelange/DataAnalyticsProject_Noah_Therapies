-- The objective is to get an overview of new and returning users per month. 

SELECT 
  *,
  Returning_users - (Returning_users + New_users - total_users) AS final_returning
  FROM (
    SELECT
    comb_ym, 
    COUNT(DISTINCT userId) AS total_users,
    COUNT(DISTINCT CASE WHEN Visitor_Type = 'returning' THEN userId END) AS Returning_users,
    COUNT(DISTINCT CASE WHEN Visitor_Type = 'new' THEN userId END) AS New_users,
    FROM {{ ref('src_visit_logs') }}
    GROUP BY comb_ym
    )
JOIN {{ ref('src_visit_logs') }}
USING (comb_ym)