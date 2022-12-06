SELECT *
FROM {{ source('staging','patient_group_ref') }}
