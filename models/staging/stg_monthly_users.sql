SELECT 
  year,
  month,
  CONCAT(year,"-", month) AS comb_ym,
  COUNT(DISTINCT userId) AS nb_monthly_users
FROM {{ ref('src_visit_logs') }}
GROUP BY year, month
ORDER BY year, month ASC