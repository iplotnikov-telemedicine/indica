
{{ config(materialized='ephemeral') }}


with int_office_integrations_stat as (

    select * from {{ ref('int_office_integrations_stat')}}

),

final as (

    select

        comp_id,
        COUNT(leafly_sync_id) as leafly_sync_offices_count,
        COUNT(weedmaps_menu_sync_id) as weedmaps_sync_offices_count

    from int_office_integrations_stat

    group by 1

)

select * from final