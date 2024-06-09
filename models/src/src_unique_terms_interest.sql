WITH unique_term_interest AS(
    SELECT * FROM {{ source('dbt', 'terms_classification')}}
)
SELECT *

FROM
    unique_term_interest