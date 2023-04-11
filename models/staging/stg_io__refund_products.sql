SELECT *
FROM {{ source('staging','refund_products') }}
