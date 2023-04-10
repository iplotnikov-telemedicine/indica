SELECT *
FROM {{ source('staging','product_prices') }}
