-- Definition of the product stickiness within a CTE
WITH df_product_stickiness AS (
    SELECT
    s.comb_ym,
    ROUND(avg_weekly_users / p.nb_monthly_users, 4) AS product_stickiness
FROM (
    SELECT -- inner subquery
        comb_ym,
        AVG(nb_weekly_users) AS avg_weekly_users
    FROM {{ ref('stg_weekly_users') }}
    GROUP BY comb_ym
) AS s
JOIN {{ ref('stg_monthly_users') }} AS p
USING (comb_ym)
)

-- Join results with main visit logs
SELECT s.*, ps.product_stickiness
FROM df_product_stickiness AS ps
LEFT JOIN {{ ref('src_visit_logs') }} AS s
USING (comb_ym)