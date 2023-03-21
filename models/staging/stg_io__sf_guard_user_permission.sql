SELECT *
FROM {{ source('staging','sf_guard_user_permission') }}