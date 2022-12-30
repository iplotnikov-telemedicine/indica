SELECT *
FROM {{ source('staging','product_filter_index') }}
