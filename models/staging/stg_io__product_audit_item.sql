SELECT *
FROM {{ source('staging','product_audit_item') }}
