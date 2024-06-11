WITH avg_weekly_users AS (
  SELECT
    comb_ym,
    AVG(nb_weekly_users) AS avg_weekly_users
  FROM {{ ref('stg_weekly_users') }}
  GROUP BY comb_ym
  ORDER BY comb_ym ASC
),

-- Join average weekly users with number of monthly users.
comb AS (
  SELECT
  s.*,
  p.nb_monthly_users
  FROM avg_weekly_users AS s
  JOIN {{ ref('stg_monthly_users') }} AS p
  USING (comb_ym)
),

-- Calculate product stickiness: % likelihood a user would use the app on a weekly basis. 
product_stickiness AS (
    SELECT 
    *,
    ROUND(avg_weekly_users / nb_monthly_users, 4) AS product_stickiness
FROM comb
)

--Merge with visit logs
SELECT *
FROM product_stickiness
JOIN {{ ref('src_visit_logs') }}
USING (comb_ym)