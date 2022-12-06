SELECT *
FROM {{ source('staging','product_transactions') }}
