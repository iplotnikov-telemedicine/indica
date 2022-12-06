
{{ config(materialized='ephemeral') }}


with registers as (

    select * from {{ ref('stg_io__registers')}}

),

register_types as (

    select * from {{ ref('register_types')}}

),

final as (

    select

        comp_id,
        COUNT(*) as total_registers_count,
        COUNT(CASE register_types.type_name WHEN 'POS' THEN 1 END) as pos_registers_count,
        COUNT(CASE register_types.type_name WHEN 'Mobile' THEN 1 END) as mobile_registers_count,
        COUNT(CASE register_types.type_name WHEN 'TV' THEN 1 END) as tv_registers_count

    from registers
    inner join register_types
        on registers.type = register_types.type_id

    group by 1

)

select * from final