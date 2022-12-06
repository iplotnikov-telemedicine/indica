
{{ config(materialized='ephemeral') }}


with offices as (

    select * from {{ ref('stg_io__offices')}}

),

final as (

    select

        office_comp_id as comp_id,
        COUNT(CASE selling_type WHEN 'office' THEN 1 END) as actual_storefront_offices_count,
        COUNT(CASE selling_type WHEN 'ondemand_office' THEN 1 END) as actual_ondemand_offices_count,
        COUNT(CASE selling_type WHEN 'ondemand_storage' THEN 1 END) as actual_ondemand_storages_count,
        COUNT(CASE selling_type WHEN 'storage' THEN 1 END) as actual_ordinary_storages_count,
        COUNT(CASE is_potify WHEN 1 THEN 1 END) as actual_sweede_offices_count,
        COUNT(CASE potify_is_accept_order WHEN 1 THEN 1 END) as actual_sweede_accept_offices_count

    from offices

    group by 1

)

select * from final