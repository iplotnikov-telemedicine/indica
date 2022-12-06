SELECT *
FROM {{ source('staging','patient_group') }}
