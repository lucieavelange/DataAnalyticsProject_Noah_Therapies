-- The objective is to calculate churn, considering a churned user as inactive for at least a month. And get a monthly chrn rate to analyze the evolution overtime. 

--Define the churn calculation.
WITH churn AS (
    SELECT *, 
    CASE 
        WHEN NOT EXISTS (
            SELECT 1 
            FROM dbtprojectnoah.dbt.dim_visit_logs_date_format ua2
            WHERE ua2.userId = ua.userId AND ua2.date > ua.date
        ) THEN 'churned'
        ELSE ''
    END AS churn_status,
    DATE_ADD(date, INTERVAL 1 MONTH) AS new_date_churn 
FROM dbtprojectnoah.dbt.dim_visit_logs_date_format ua
),

--Review churn date format.
date_date AS (
    SELECT *,
    FORMAT_DATE('%Y', new_date_churn) AS churn_year,
    FORMAT_DATE('%m', new_date_churn) AS month_churned
FROM churn
)

--Concatenate churn year and month. This way, the monthly churn can be easily calculated. 
SELECT *,
    CONCAT(churn_year, "-", month_churned) AS comb_ym_churn
FROM date_date