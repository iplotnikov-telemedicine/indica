SELECT *
FROM {{ source('staging','product_checkins') }}
