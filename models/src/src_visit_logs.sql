WITH visit_logs AS(
    SELECT * FROM {{ source('dbtproject', 'visits')}}
)
SELECT *

FROM
    visit_logs
