
{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key=['comp_id', 'id'],
        sort=['comp_id', 'id'],
        dist='updated_at'
    )
}}


with order_items as (

    select * 
    from {{ ref('stg_io__warehouse_order_items') }}
    where count > 0
    {% if is_incremental() %}
        and updated_at > (select max(updated_at) from {{ this }})
        or comp_id not in (select distinct comp_id from {{ this }})
    {% endif %}

),

discounts as (

    select * from {{ ref('stg_io__discounts') }}

),

order_items_with_orders as (

    select
        orders.domain_prefix,
        orders.office_id,
        orders.office_name,

        order_items.*,
        1.0 * order_items.amount / NULLIF(SUM(order_items.amount) OVER (partition by order_items.comp_id, order_items.order_id), 0) as share_in_order_amount,

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
        orders.patient_id,
        orders.shipping_amount as delivery_fee

    from {{ ref('orders_with_details') }} orders

    inner join order_items
        on orders.comp_id = order_items.comp_id
        and orders.id = order_items.order_id

    left join discounts as item_discounts
        on order_items.discount_id = item_discounts.id
        and order_items.comp_id = item_discounts.comp_id
        and item_discounts.apply_type = 'item'

    where confirmed_at is not null

),

products_with_details as (

    select * from {{ ref('int_products_with_details') }}

),



product_checkins as (

    select * from {{ ref('stg_io__product_checkins') }}

),

final as (

    select
        order_items_with_orders.*,        
        pwd.net_weight,
        pwd.prod_cost,
        pwd.prod_sku,
        pwd.brand_name,
        pwd.direct_category as product_direct_category,
        pwd.parent_category as product_parent_category,
        pwd.sub_category_1 as product_sub_category_1,
        pwd.sub_category_2 as product_sub_category_2,
        product_checkins.vendor_id,
        product_checkins.vendor_name
        
    from order_items_with_orders

    left join products_with_details pwd
        on order_items_with_orders.product_id = pwd.prod_id
        and order_items_with_orders.comp_id = pwd.comp_id

    left join product_checkins
        on order_items_with_orders.comp_id = product_checkins.comp_id
        and order_items_with_orders.product_checkin_id = product_checkins.id  

)

select * from final