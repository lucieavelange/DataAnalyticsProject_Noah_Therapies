-- The objective is to get an overview of new and returning users per month. 

-- Format date.
WITH f_date_format AS (
  SELECT 
    *, 
    FORMAT_DATE('%Y', date) AS f_year, 
    FORMAT_DATE('%m', date) AS f_month
  FROM dbtprojectnoah.dbt.dim_visit_logs_date_format
),

-- Define date format to get the month-year dimension.
concat AS (
  SELECT 
    *, 
    CONCAT(f_year, "-", f_month) AS f_comb_ym
  FROM f_date_format
),

--Calculate number of new and returning users per month.
type AS (
  SELECT
    f_comb_ym,
    COUNT(DISTINCT userId) AS total_users,
    COUNT(DISTINCT CASE WHEN Visitor_Type = 'returning' THEN userId END) AS Returning_users,
    COUNT(DISTINCT CASE WHEN Visitor_Type = 'new' THEN userId END) AS New_users
  FROM concat
  GROUP BY f_comb_ym
),

-- Define a test on the total number of users.
type_final AS (
  SELECT 
    *,
    Returning_users + New_users AS total_check,
    Returning_users + New_users - total_users AS difference
  FROM type
),

final AS (
  SELECT 
  *,
  Returning_users - difference AS final_returning
FROM type_final
)

SELECT *
FROM final AS p
JOIN dbtprojectnoah.dbt.dim_visit_logs_date_format AS s
ON p.f_comb_ym=s.comb_ym