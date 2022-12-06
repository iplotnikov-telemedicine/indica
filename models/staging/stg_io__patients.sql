SELECT *
FROM {{ source('staging','patients') }}
