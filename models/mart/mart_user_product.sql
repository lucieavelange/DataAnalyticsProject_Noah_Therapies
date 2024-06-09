-- The objective is to build graphs showing the average number of visits and average number of days spent on the app per age category and health condition. To do so, the latest cumulated time and number of visits per user need to be the only ones selected, and then group by the needed dimensions. 

--Define the age category. 
WITH joined AS (
    SELECT s.*,
    p.age,
    p.basicCondition 
    FROM dbtprojectnoah.dbt.dim_visit_logs_date_format AS s
    JOIN dbtprojectnoah.source.source_user_data AS p
    USING (userId)
),

category AS (
    SELECT *,
        CASE
            WHEN age BETWEEN 18 AND 24 THEN '18-24'
            WHEN age BETWEEN 25 AND 34 THEN '25-34'
            WHEN age BETWEEN 35 AND 44 THEN '35-44'
            WHEN age BETWEEN 45 AND 54 THEN '45-54'
            WHEN age > 54 THEN '55+'
            ELSE 'Unknown'
        END AS age_category,
    FROM joined
),

-- Define latest time and number of visits per user.
def_latest AS (
    SELECT
        userId,
        age,
        age_category,
        basicCondition,
        Total_cumulated_session_length__in_sec_,
        Nb_visits,
        ROW_NUMBER() OVER (PARTITION BY userId ORDER BY Total_cumulated_session_length__in_sec_ DESC) AS rank_time,
        ROW_NUMBER() OVER (PARTITION BY userId ORDER BY Nb_visits DESC) AS rank_visits
    FROM
        category
)

SELECT *
FROM def_latest
  WHERE rank_time=1 AND rank_visits=1 -- The table keeps only the MAX figures per userId. The average calculation and group-by function are done on Looker. 