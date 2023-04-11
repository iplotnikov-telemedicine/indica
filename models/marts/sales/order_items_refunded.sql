
{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key=['comp_id', 'id'],
        sort=['comp_id', 'id'],
        dist='updated_at'
    )
}}


with items_returned as (

    SELECT
        comp_id,
        order_item_id,
        max(refunded_at) as refunded_at,
        max(updated_at) as updated_at,
        sum(refund_qty) as returned_quantity,
        sum(refund_amount) as returned_amount

    FROM {{ ref('stg_io__refund_products') }} refund_products

    WHERE 1=1
    {% if is_incremental() %}
        and updated_at > (select max(updated_at) from {{ this }})
    {% endif %}

    GROUP BY 1, 2

),

returned_with_details as (

    SELECT
        items_returned.comp_id,
        items_returned.order_item_id as id,
        items_returned.returned_quantity,
        items_returned.returned_amount,
        items_returned.refunded_at as returned_at,
        items_returned.updated_at,
        order_items.product_id,
        order_items.paid_amount,
        order_items.tax

    FROM {{ ref('stg_io__warehouse_order_items') }} order_items

    INNER JOIN items_returned
        ON order_items.comp_id = items_returned.comp_id
        AND order_items.id = items_returned.order_item_id

),

final as (

    SELECT
        returned_with_details.comp_id,
        returned_with_details.id,
        returned_with_details.product_id,
        returned_with_details.returned_quantity,
        returned_with_details.returned_amount,
        returned_with_details.returned_at,
        COALESCE(CASE products.is_excise WHEN 1 
            THEN tax_payment.excise_tax * returned_with_details.returned_amount / NULLIF(returned_with_details.paid_amount, 0)
            ELSE 0 
            END, 0) as refund_sdp_excise_tax,
        COALESCE(CASE products.is_excise WHEN 1 
            THEN 0
            ELSE tax_payment.excise_tax * returned_with_details.returned_amount / NULLIF(returned_with_details.paid_amount, 0)
            END, 0) as refund_nsdp_excise_tax,
        returned_with_details.returned_amount * returned_with_details.tax / NULLIF(returned_with_details.paid_amount, 0) as tax_refunded,
        returned_with_details.returned_amount - tax_refunded as refund_wo_tax,
        returned_with_details.updated_at

    FROM returned_with_details

    INNER JOIN {{ ref('int_products_with_details') }} products
        ON returned_with_details.comp_id = products.comp_id
        AND returned_with_details.product_id = products.prod_id

    INNER JOIN {{ ref('stg_io__tax_payment') }} tax_payment
        ON returned_with_details.comp_id = tax_payment.comp_id
        AND returned_with_details.id = tax_payment.order_item_id
    
)

SELECT * FROM final
