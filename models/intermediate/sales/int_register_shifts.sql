
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
        comp_id,
        id as register_id,
        register_log.created_at as current_created_at,
        register_log.type as current_type,
        lag(created_at) over (partition by comp_id, register_id order by created_at) as lag_created_at,
        lag(type) over (partition by comp_id, register_id order by created_at) as lag_type

    FROM register_log

),

offices as (

    select * 
    from {{ ref('stg_io__offices') }}
    
),

final as (

    SELECT
        oac.comp_id,
        oac.register_id,
        r.name as register_name,
        r.office_id,
        oac.lag_created_at as open_at,
        oac.current_created_at as closed_at

    FROM openings_and_closings oac

    INNER JOIN registers r
        ON oac.register_id = r.id
        AND oac.comp_id = r.comp_id

    WHERE oac.lag_type = 1 and oac.current_type = 4

)

SELECT * FROM final