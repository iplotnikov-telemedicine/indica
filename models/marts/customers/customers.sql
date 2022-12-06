
{{
    config(
        materialized='table',
        sort='comp_id',
        dist='comp_id'
    )
}}


with customers as (

    select * from {{ ref('int_customers') }}

),

subscription_plans as (

    select * from {{ ref('stg_io__subscription_plans') }}

),

country as (

    select * from {{ ref('stg_io__country') }}

),

int_customer_integrations_stat as (

    select * from {{ ref('int_customer_integrations_stat') }}

),

int_customer_offices_stat as (

    select * from {{ ref('int_customer_offices_stat') }}

),

int_customer_patients_stat as (

    select * from {{ ref('int_customer_patients_stat') }}

),

int_customer_products_stat as (

    select * from {{ ref('int_customer_products_stat') }}

),

int_customer_registers_stat as (

    select * from {{ ref('int_customer_registers_stat') }}

),

int_customer_sales_stat as (

    select * from {{ ref('int_customer_sales_stat') }}

),

final as (

    select

        customers.*,
        
        country.name as country_name, 

        subscription_plans.name as subscription_name,

        coalesce(int_customer_integrations_stat.leafly_sync_offices_count, 0) as leafly_sync_offices_count,
        coalesce(int_customer_integrations_stat.weedmaps_sync_offices_count, 0) as weedmaps_sync_offices_count,

        coalesce(int_customer_offices_stat.actual_storefront_offices_count, 0) as actual_storefront_offices_count,
        coalesce(int_customer_offices_stat.actual_ondemand_offices_count, 0) as actual_ondemand_offices_count,
        coalesce(int_customer_offices_stat.actual_ondemand_storages_count, 0) as actual_ondemand_storages_count,
        coalesce(int_customer_offices_stat.actual_ordinary_storages_count, 0) as actual_ordinary_storages_count,
        coalesce(int_customer_offices_stat.actual_sweede_offices_count, 0) as actual_sweede_offices_count,
        coalesce(int_customer_offices_stat.actual_sweede_accept_offices_count, 0) as actual_sweede_accept_offices_count,

        coalesce(int_customer_patients_stat.total_patients_count, 0) as total_patients_count,
        coalesce(int_customer_patients_stat.male_patients_count, 0) as male_patients_count,
        coalesce(int_customer_patients_stat.female_patients_count, 0) as female_patients_count,
        coalesce(int_customer_patients_stat.unspecified_patients_count, 0) as unspecified_patients_count,

        coalesce(int_customer_products_stat.total_products_count, 0) as total_products_count,
        coalesce(int_customer_products_stat.mj_products_count, 0) as mj_products_count,
        coalesce(int_customer_products_stat.non_mj_products_count, 0) as non_mj_products_count,

        coalesce(int_customer_registers_stat.total_registers_count, 0) as total_registers_count,
        coalesce(int_customer_registers_stat.pos_registers_count, 0) as pos_registers_count,
        coalesce(int_customer_registers_stat.mobile_registers_count, 0) as mobile_registers_count,
        coalesce(int_customer_registers_stat.tv_registers_count, 0) as tv_registers_count,

        int_customer_sales_stat.min_report_date,
        int_customer_sales_stat.max_report_date,
        coalesce(int_customer_sales_stat.months_with_indica, 0) as months_with_indica,
        coalesce(int_customer_sales_stat.sweede_sales_offices_count, 0) as sweede_sales_offices_count

    from customers

    inner join subscription_plans
        on customers.plan = subscription_plans.id

    left join country
        on customers.country_id = country.id

    left join int_customer_integrations_stat
        on customers.comp_id = int_customer_integrations_stat.comp_id

    left join int_customer_offices_stat
        on customers.comp_id = int_customer_offices_stat.comp_id

    left join int_customer_patients_stat
        on customers.comp_id = int_customer_patients_stat.comp_id

    left join int_customer_products_stat
        on customers.comp_id = int_customer_products_stat.comp_id

    left join int_customer_registers_stat
        on customers.comp_id = int_customer_registers_stat.comp_id

    left join int_customer_sales_stat
        on customers.comp_id = int_customer_sales_stat.comp_id

)

select * from final