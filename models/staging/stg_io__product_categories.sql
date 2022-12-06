SELECT *
FROM {{ source('staging','product_categories') }}
