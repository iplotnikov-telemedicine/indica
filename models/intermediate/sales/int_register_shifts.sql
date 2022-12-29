
{{
    config(
        materialized='table',
        unique_key=['comp_id', 'register_id', 'open_at'],
        sort=['comp_id', 'register_id'],
        dist='open_at'
    )
}}


with register_log as (

    select * 
    from {{ ref('stg_io__register_log') }}
    
    WHERE type in (1, 4)

),

registers as (

    select * 
    from {{ ref('stg_io__registers') }}

    WHERE type = 1  -- only POS registers
    
),

openings_and_closings as (
    
    SELECT
        r.comp_id,
        r.id as register_id,
        r.name as register_name,
        register_log.created_at as current_created_at,
        register_log.type as current_type,
        lag(register_log.created_at) over (partition by register_log.comp_id, register_log.register_id order by register_log.created_at) as lag_created_at,
        lag(register_log.type) over (partition by register_log.comp_id, register_log.register_id order by register_log.created_at) as lag_type

    FROM register_log

    INNER JOIN registers r
        ON register_log.register_id = r.id
        AND register_log.comp_id = r.comp_id

),

final as (

    SELECT
        comp_id,
        register_id,
        register_name,
        lag_created_at as open_at,
        current_created_at as closed_at

    FROM openings_and_closings

    WHERE lag_type = 1 and current_type = 4

)

SELECT * FROM final