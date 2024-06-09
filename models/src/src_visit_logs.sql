WITH visit_logs AS(
    SELECT * FROM {{ source('dbtprojectnoah', 'visits')}}
)
SELECT *

FROM
    visit_logs
