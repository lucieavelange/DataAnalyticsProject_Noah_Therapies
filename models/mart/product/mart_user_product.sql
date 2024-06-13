-- The objective is to build graphs showing the average number of visits and average number of days spent on the app per age category and health condition. To do so, the latest cumulated time and number of visits per user need to be the only ones selected, and then group by the needed dimensions. 

SELECT 
sub.*,
CASE
    WHEN sub.age BETWEEN 18 AND 24 THEN '18-24'
    WHEN sub.age BETWEEN 25 AND 34 THEN '25-34'
    WHEN sub.age BETWEEN 35 AND 44 THEN '35-44'
    WHEN sub.age BETWEEN 45 AND 54 THEN '45-54'
    WHEN sub.age > 54 THEN '55+'
    ELSE 'Unknown'
END AS age_category,
FROM (
    SELECT 
        s.*,
        p.age,
        p.basicCondition,
        ROW_NUMBER() OVER (PARTITION BY s.userId ORDER BY s.Total_cumulated_session_length__in_sec_ DESC) AS rank_time,
        ROW_NUMBER() OVER (PARTITION BY s.userId ORDER BY Nb_visits DESC) AS rank_visits
    FROM {{ ref('src_visit_logs') }} AS s
    JOIN {{ ref('src_user_data') }} AS p
    USING (userId)
) AS sub
WHERE sub.rank_time = 1 AND sub.rank_visits = 1