
{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key=['comp_id', 'id'],
        sort=['comp_id', 'id'],
        dist='updated_at'
    )
}}


with orders as (

    select * 
    from {{ ref('stg_io__warehouse_orders') }}
    {% if is_incremental() %}
        where updated_at > (select max(updated_at) from {{ this }})
        or comp_id not in (select distinct comp_id from {{ this }})
    {% endif %}

),

patients as (

    select * from {{ ref('stg_io__patients') }}

),

patient_group_mapping as (

    select * from {{ ref('stg_io__patient_group_mapping') }}

),

patient_groups as (

    select * from {{ ref('stg_io__patient_groups') }}

),

patients_with_groups as (

    select
        patients.comp_id,
        patients.pat_id as patient_id,
        patients.pat_first_name as patient_first_name,
        patients.pat_last_name as patient_last_name,
        patients.pat_phone as patient_phone,
        NOT patients.phone_consent_given_at IS NULL as patient_has_phone_consent,
        patients.pat_state_name as patient_state_name,
        patients.pat_city_name as patient_city_name,
        patients.pat_dmv as patient_dmv,
        patients.pat_zip_name as patient_zip_name,
        listagg(patient_groups.name, ', ')  as patient_groups

    from patients

    left join patient_group_mapping pgm
        on patients.comp_id = pgm.comp_id
        and patients.pat_id = pgm.patient_id

    left join patient_groups
        on pgm.comp_id = patient_groups.comp_id
        and pgm.group_id = patient_groups.id

    group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10

),

customers as (

    select * from {{ ref('int_customers') }}

),

offices as (

    select * from {{ ref('stg_io__offices') }}

),

discounts as (

    select * from {{ ref('stg_io__discounts') }}

),

final as (

    select
        customers.domain_prefix,

        offices.office_name,

        orders.*,

        cart_discounts.name as cart_discount_name,

        patients_with_groups.patient_first_name,
        patients_with_groups.patient_last_name,
        patients_with_groups.patient_phone,
        patients_with_groups.patient_has_phone_consent,
        patients_with_groups.patient_state_name,
        patients_with_groups.patient_city_name,
        patients_with_groups.patient_dmv,
        patients_with_groups.patient_zip_name,
        patients_with_groups.patient_groups

    from orders
    
    inner join customers
        on orders.comp_id = customers.comp_id
    
    inner join offices
        on orders.office_id = offices.office_id
    
    left join patients_with_groups
        on orders.patient_id = patients_with_groups.patient_id
        and orders.comp_id = patients_with_groups.comp_id

    left join discounts as cart_discounts
        on orders.discount_id = cart_discounts.id
        and orders.comp_id = cart_discounts.comp_id
        and cart_discounts.apply_type = 'cart'

)

select * from final