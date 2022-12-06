SELECT *
FROM {{ source('staging','warehouse_orders') }}
