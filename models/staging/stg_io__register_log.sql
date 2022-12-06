SELECT *
FROM {{ source('staging','register_log') }}
