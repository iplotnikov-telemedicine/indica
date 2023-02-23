
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
    where 1=1
    {% if is_incremental() %}
        and inserted_at > (select max(inserted_at) from {{ this }})
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

        orders.comp_id, 
        orders.id, 
        orders."number", 
        orders.patient_id, 
        orders."type", 
        orders.status, 
        orders.order_status, 
        orders.payment_status, 
        orders.fulfillment_status, 
        orders.shipment_status, 
        orders.created_at, 
        orders.updated_at, 
        orders.charge_by, 
        orders.amount, 
        orders.referral_discount_value, 
        orders.discount_type_bak, 
        orders.total_amount, 
        orders.discount_has_changed, 
        orders.office_id, 
        orders.sum_tax, 
        orders.sum_discount, 
        orders.sum_free_discount, 
        orders.sum_income, 
        orders.custom_discount_value, 
        orders.custom_discount_type_bak, 
        orders.delivery_address, 
        orders.delivery_city, 
        orders.delivery_state, 
        orders.delivery_zip, 
        orders.delivery_phone, 
        orders.delivery_latitude, 
        orders.delivery_longitude, 
        orders.shipping_method_id, 
        orders.shipping_amount, 
        orders.courier_register_id, 
        orders."comment", 
        orders.sync_updated_at, 
        orders.sync_created_at, 
        orders.register_id, 
        orders.discount_id, 
        orders.referral_discount_type, 
        orders.custom_discount_type, 
        orders.balance, 
        orders.method1_amount, 
        orders.method2_amount, 
        orders.method3_amount, 
        orders.method4_amount, 
        orders.method5_amount, 
        orders.method6_amount, 
        orders.method7_amount, 
        orders.processing_register_id, 
        orders.photo, 
        orders.delivery_datetime, 
        orders.delivery_address_id, 
        orders.change_amount, 
        orders.tip_amount, 
        orders.placed_at, 
        orders.completed_at, 
        orders.confirmed_at, 
        orders.preferred_payment_method, 
        orders.is_bonus_point_as_discount, 
        orders.marketplace, 
        orders.applied_potify_credits, 
        orders.asap_delivery, 
        orders.cashier_id, 
        orders.is_transit_started, 
        orders.metrc_status, 
        orders.cashier_name, 
        orders.patient_type, 
        orders.register_name, 
        orders.courier_id, 
        orders.courier_name, 
        orders.courier_register_name, 
        orders.is_verified_by_courier, 
        orders.is_shipped, 
        orders.shipping_tracking_number, 
        orders.patient_has_caregiver, 
        orders.patient_is_tax_exempt, 
        orders.metrc_substatus, 
        orders.checkout_staff_id, 
        orders.pos_mode, 
        orders.signature, 
        orders.delivery_method, 
        orders.courier_number, 
        orders.patient_rec_number, 
        orders.office_zip_name, 
        orders.refund_type, 
        orders.returned_at, 
        orders.shipping_method_name, 
        orders.tax_tier_version_id, 
        orders.vehicle, 
        orders.metrc_delivery_status, 
        orders.resend_staff_id, 
        orders.delivery_estimated_time_of_arrival, 

        cart_discounts.name as cart_discount_name,

        patients_with_groups.patient_first_name,
        patients_with_groups.patient_last_name,
        patients_with_groups.patient_phone,
        patients_with_groups.patient_has_phone_consent,
        patients_with_groups.patient_state_name,
        patients_with_groups.patient_city_name,
        patients_with_groups.patient_dmv,
        patients_with_groups.patient_zip_name,
        patients_with_groups.patient_groups,

        orders.inserted_at

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