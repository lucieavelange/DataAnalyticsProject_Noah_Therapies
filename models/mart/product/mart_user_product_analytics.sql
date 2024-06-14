SELECT 
    userId AS userId,
    age,
    CASE
        WHEN age BETWEEN 18 AND 24 THEN '18-24'
        WHEN age BETWEEN 25 AND 34 THEN '25-34'
        WHEN age BETWEEN 35 AND 44 THEN '35-44'
        WHEN age BETWEEN 45 AND 54 THEN '45-54'
        WHEN age > 54 THEN '55+'
        ELSE 'Unknown'
    END AS age_category,
    CASE
        WHEN basicCondition IS NOT NULL THEN basicCondition
        ELSE 'Unknown'
    END AS basicCondition_category,
    SUM(Visit_Duration__in_sec_) AS time_spent_in_sec,
    COUNT(CASE WHEN Visit_Duration__in_sec_ != 0 THEN userId ELSE NULL END) AS nb_visits
FROM 
    {{ ref('stg_product_user_combined') }}
GROUP BY 
    userId, 
    age,
    age_category,
    basicCondition_category