
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
    {% endif %}
),

orders as (

    select * from {{ ref('orders_with_details') }} where confirmed_at is not null

),

products_with_details as (

    select * from {{ ref('int_products_with_details') }}

),

discounts as (

    select * from {{ ref('stg_io__discounts') }}

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

        pwd.net_weight,
        pwd.prod_cost,
        pwd.prod_sku,
        pwd.brand_name,
        pwd.vendor_name,
        pwd.direct_category as product_direct_category,
        pwd.parent_category as product_parent_category,
        pwd.sub_category_1 as product_sub_category_1,
        pwd.sub_category_2 as product_sub_category_2
        
        
    from order_items
    
    inner join orders
        on order_items.comp_id = orders.comp_id
        and order_items.order_id = orders.id

    left join products_with_details pwd
        on order_items.product_id = pwd.prod_id
        and order_items.comp_id = pwd.comp_id

    left join discounts as item_discounts
        on order_items.discount_id = item_discounts.id
        and order_items.comp_id = item_discounts.comp_id
        and item_discounts.apply_type = 'item'

)

select * from final