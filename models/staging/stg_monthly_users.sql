SELECT 
  year,
  month,
  CONCAT(year,"-", month) AS comb_ym,
  COUNT(DISTINCT userId) AS nb_monthly_users
FROM dbtprojectnoah.dbt.dim_visit_logs_date_format
GROUP BY year, month
ORDER BY year, month ASC