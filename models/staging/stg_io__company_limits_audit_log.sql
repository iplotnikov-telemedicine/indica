{{
    config(
        materialized='view'
    )
}}

select
    *
FROM {{ source('ext_indica_backend','company_limits_audit_log') }}