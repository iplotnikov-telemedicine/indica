SELECT *
FROM {{ source('staging','product_price_group') }}
