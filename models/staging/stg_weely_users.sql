SELECT 
  year,
  month,
  week_number,
  CONCAT(year,"-", month) AS comb_ym,
  COUNT(DISTINCT userId) AS nb_weekly_users
FROM dbtprojectnoah.dbt.dim_visit_logs_date_format
GROUP BY year, month, week_number
ORDER BY year, month, week_number ASC