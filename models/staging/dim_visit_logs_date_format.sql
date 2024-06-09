WITH date_format AS (
  SELECT
  *,
  FORMAT_DATE('%Y', date) AS year,
  FORMAT_DATE('%m', date) AS month,
  CEIL(EXTRACT(DAY FROM date) / 7) AS week_number
FROM dbtprojectnoah.source.source_visit_logs
)

SELECT *,
CONCAT(year,"-", month) AS comb_ym
FROM date_format