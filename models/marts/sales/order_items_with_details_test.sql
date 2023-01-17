{{
    config(
        schema='marts',
        materialized='insert_by_period',
        period='week',
        timestamp_field='updated_at',
        start_date='2019-01-01',
        unique_key=['comp_id', 'id'],
        sort=['comp_id', 'id'],
        dist='updated_at'
    )
}}

with order_items as (
    select * 
    from {{ ref('stg_io__warehouse_order_items') }}
    where count > 0 and __PERIOD_FILTER__
),

orders as (

    select * 
    from {{ ref('orders_with_details') }} 
    where confirmed_at is not null

),

products_with_details as (

    select * from {{ ref('int_products_with_details') }}

),

discounts as (

    select * from {{ ref('stg_io__discounts') }}

),

product_checkins as (

    select * from {{ ref('stg_io__product_checkins') }}

),

final as (

    select
        orders.domain_prefix,
        orders.office_id,
        orders.office_name,

        order_items.*,
        
        orders.sum_discount as total_discount_amount,
        item_discounts.name as item_discount_name,
        order_items.discount_amount as item_discount_amount,
        orders.cart_discount_name,
        total_discount_amount - (sum(item_discount_amount) over (partition by orders.comp_id, orders.id)) as cart_discount_amount,

        orders.number as order_number,
        orders.confirmed_at,
        orders.cashier_name,
        orders.sum_tax as order_sum_tax,
        orders.total_amount as order_total_amount,
        orders.patient_first_name || ' ' || orders.patient_last_name as patient_full_name,
        orders.patient_state_name,
        orders.patient_city_name,
        orders.patient_dmv,
        orders.patient_zip_name,
        orders.patient_groups,
        
        product_checkins.vendor_id,
        product_checkins.vendor_name
        
    from order_items
    
    inner join orders
        on order_items.comp_id = orders.comp_id
        and order_items.order_id = orders.id

    left join discounts as item_discounts
        on order_items.discount_id = item_discounts.id
        and order_items.comp_id = item_discounts.comp_id
        and item_discounts.apply_type = 'item'
    
    left join product_checkins
        on order_items.comp_id = product_checkins.comp_id
        and order_items.product_checkin_id = product_checkins.id

)

select * from final