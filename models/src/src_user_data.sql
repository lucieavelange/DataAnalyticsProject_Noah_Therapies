WITH user_data AS(
    SELECT * FROM {{ source('dbt', 'user_data')}}
)
SELECT *

FROM
    user_data