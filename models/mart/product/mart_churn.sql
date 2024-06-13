-- The objective is to calculate churn, considering a churned user as inactive for at least a month. And get a monthly chrn rate to analyze the evolution overtime. 

--Define the churn calculation.
WITH churn AS (
    SELECT *, 
    CASE 
        WHEN NOT EXISTS (
            SELECT 1 
            FROM {{ ref('src_visit_logs') }} ua2
            WHERE ua2.userId = ua.userId AND ua2.date > ua.date
        ) THEN 'churned'
        ELSE ''
    END AS churn_status,
    DATE_ADD(date, INTERVAL 1 MONTH) AS new_date_churn 
FROM {{ ref('src_visit_logs') }} ua
)

SELECT *,
       CONCAT(FORMAT_DATE('%Y', new_date_churn), "-", FORMAT_DATE('%m', new_date_churn)) AS comb_ym_churn --outer query
FROM (
    SELECT *, --inner query
           FORMAT_DATE('%Y', new_date_churn) AS churn_year,
           FORMAT_DATE('%m', new_date_churn) AS month_churned
    FROM churn
) AS date_date