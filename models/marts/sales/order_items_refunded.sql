
{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        unique_key=['comp_id', 'id'],
        sort=['comp_id', 'id'],
        dist='inserted_at'
    )
}}


with order_items as (

    select * 
    from {{ ref('stg_io__warehouse_order_items') }}
    where is_returned = 1
    {% if is_incremental() %}
        and inserted_at > (select max(inserted_at) from {{ this }})
    {% endif %}

),

products as (

    select * from {{ ref('int_products_with_details') }}

),

tax_payment as (

    select * from {{ ref('stg_io__tax_payment') }}

),

final as (

    SELECT
        order_items.comp_id,
        order_items.id,
        order_items.product_id,
        (order_items.qty + order_items.qty_free) * order_items.count as returned_quantity,
        order_items.returned_amount,
        order_items.returned_at,
        COALESCE(CASE products.is_excise WHEN 1 
            THEN tax_payment.excise_tax * order_items.returned_amount / NULLIF(order_items.paid_amount, 0)
            ELSE 0 
            END, 0) as refund_sdp_excise_tax,
        COALESCE(CASE products.is_excise WHEN 1 
            THEN 0
            ELSE tax_payment.excise_tax * order_items.returned_amount / NULLIF(order_items.paid_amount, 0)
            END, 0) as refund_nsdp_excise_tax,
        order_items.returned_amount * order_items.tax / NULLIF(order_items.paid_amount, 0) as tax_refunded,
        order_items.returned_amount - tax_refunded as refund_wo_tax,
        order_items.inserted_at

    FROM order_items

    INNER JOIN products
        ON order_items.comp_id = products.comp_id
        AND order_items.product_id = products.prod_id

    INNER JOIN tax_payment
    ON order_items.comp_id = tax_payment.comp_id
        AND order_items.id = tax_payment.order_item_id
    
)

SELECT * FROM final
