
{{
    config(
        materialized='table',
        unique_key=['comp_id', 'register_id', 'open_at'],
        sort=['comp_id', 'register_id'],
        dist='open_at'
    )
}}


with register_log as (

    SELECT * 
    FROM {{ ref('stg_io__register_log') }}
    
    WHERE type in (1, 4)

),

registers as (

    SELECT * 
    FROM {{ ref('stg_io__registers') }}

    WHERE type = 1  -- only POS registers
    
),

openings_and_closings as (
    
    SELECT
        r.comp_id,
        r.id as register_id,
        r.name as register_name,
        r.office_id,
        rl.created_at as current_created_at,
        rl.type as current_type,
        lag(rl.created_at) over (partition by rl.comp_id, rl.register_id order by rl.created_at) as lag_created_at,
        lag(rl.type) over (partition by rl.comp_id, rl.register_id order by rl.created_at) as lag_type

    FROM register_log rl

    INNER JOIN registers r
        ON rl.register_id = r.id
        AND rl.comp_id = r.comp_id


),

final as (

    SELECT
        comp_id,
        register_id,
        register_name,
        office_id,
        lag_created_at as open_at,
        current_created_at as closed_at

    FROM openings_and_closings
    
    WHERE lag_type = 1 and current_type = 4

)

SELECT * FROM final