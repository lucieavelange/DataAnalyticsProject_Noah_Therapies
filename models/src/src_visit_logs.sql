WITH visit_logs AS(
    SELECT * FROM {{ source('dbt', 'visits_date_format')}}
)
SELECT *

FROM
    visit_logs
