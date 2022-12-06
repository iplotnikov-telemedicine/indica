SELECT *
FROM {{ source('staging','warehouse_order_items') }}
