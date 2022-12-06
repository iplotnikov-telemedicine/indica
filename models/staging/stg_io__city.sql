{{
    config(
        materialized='table'
    )
}}

select
    *
FROM {{ source('ext_indica_backend','city') }}