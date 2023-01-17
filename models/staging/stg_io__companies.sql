{{
    config(
        materialized='view'
    )
}}

select
    *
FROM {{ source('ext_indica_backend','companies') }}