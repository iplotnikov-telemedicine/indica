
{{ config(materialized='table') }}


with company_config as (

    select * from {{ ref('stg_io__company_config')}}

),

parsed as (

    select

        comp_id,
        REGEXP_replace(name, '[^0-9]', '') as office_id,
        case when name like 'weedmaps_api_key%' then value end as weedmaps_api_key,
        case when name like 'weedmaps_menu_count_fact_items%' then value end as weedmaps_menu_count_fact_items,
        case when name like 'weedmaps_menu_count_plan_items%' then value end as weedmaps_menu_count_plan_items,
        case when name like 'weedmaps_menu_last_updated_count_synced_items%' then value end as weedmaps_menu_last_updated_count_synced_items,
        case when name like 'weedmaps_menu_sync_details%' then value end as weedmaps_menu_sync_details,
        case when name like 'weedmaps_menu_sync_id%' then value end as weedmaps_menu_sync_id,
        case when name like 'weedmaps_menu_threshold%' then value end as weedmaps_menu_threshold,
        case when name like 'weedmaps_menu_sync_processing_status%' then value end as weedmaps_menu_sync_processing_status,
        case when name like 'weedmaps_menu_sync_status%' then value end as weedmaps_menu_sync_status,
        case when name like 'leafly_api_key%' then value end as leafly_api_key,
        case when name like 'leafly_sync_id%' then value end as leafly_sync_id,
        case when name like 'leafly_menu_threshold%' then value end as leafly_menu_threshold,
        case when name like 'leafly_processing_status%' then value end as leafly_processing_status,
        case when name like 'leafly_sync_details%' then value end as leafly_sync_details,
        case when name like 'leafly_sync_status%' then value end as leafly_sync_status

    from company_config

    WHERE office_id <> ''
),

final as (

    select

        comp_id,
        office_id,
        max(weedmaps_api_key) as weedmaps_api_key,
        max(weedmaps_menu_count_fact_items) as weedmaps_menu_count_fact_items,
        max(weedmaps_menu_count_plan_items) as weedmaps_menu_count_plan_items,
        max(weedmaps_menu_last_updated_count_synced_items) as weedmaps_menu_last_updated_count_synced_items,
        max(weedmaps_menu_sync_details) as weedmaps_menu_sync_details,
        max(weedmaps_menu_sync_id) as weedmaps_menu_sync_id,
        max(weedmaps_menu_threshold) as weedmaps_menu_threshold,
        max(weedmaps_menu_sync_processing_status) as weedmaps_menu_sync_processing_status,
        max(weedmaps_menu_sync_status) as weedmaps_menu_sync_status,
        max(leafly_api_key) as leafly_api_key,
        max(leafly_sync_id) as leafly_sync_id,
        max(leafly_menu_threshold) as leafly_menu_threshold,
        max(leafly_processing_status) as leafly_processing_status,
        max(leafly_sync_details) as leafly_sync_details,
        max(leafly_sync_status) as leafly_sync_status

    from parsed

    group by 1, 2

)

select * from final
