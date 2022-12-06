SELECT *
FROM {{ source('staging','tax_payment') }}
