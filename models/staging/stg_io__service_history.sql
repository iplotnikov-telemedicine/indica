SELECT *
FROM {{ source('staging','service_history') }}
