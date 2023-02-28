SELECT *
FROM {{ source('staging','user_activity_record') }}