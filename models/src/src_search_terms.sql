WITH search_terms AS(
    SELECT * FROM {{ source('dbt', 'search_terms')}}
)
SELECT *

FROM
    search_terms