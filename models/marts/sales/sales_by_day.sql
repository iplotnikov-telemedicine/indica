
with order_items_with_details as (

    select * 
    from {{ ref('order_items_with_details') }} 
    where comp_id = 7520 and confirmed_at >= '2022-12-31'

), 

refunded_items_with_details as (

    select * 
    from {{ ref('order_items_with_details') }} 
    where comp_id = 7520 and returned_at >= '2022-12-31'

), 

customers AS (

    select * from {{ ref('int_customers') }}

),

timezone AS (

    select * from {{ ref('stg_io__timezone') }}

),

order_items_daily as (

    SELECT

        DATE(CONVERT_TIMEZONE('UTC', tz.zone_name, woi.confirmed_at)) as report_date,
        woi.office_id,
        woi.product_id,

        SUM(woi.paid_amount) as gross_receipts

            -- as custom_item_discounts,
            -- as custom_cart_discounts,
            -- as mark_as_free,
            -- as store_credit_used,
            -- as sweede_credit_used,

        -- SUM(shipping_amount) as delivery_fee
    
    FROM order_items_with_details woi

    INNER JOIN customers c 
        ON woi.comp_id = c.comp_id

    INNER JOIN timezone tz 
        ON c.timezone_id = tz.id

    GROUP BY 1, 2, 3


),

refunded_items_daily as (

    SELECT 

        DATE(CONVERT_TIMEZONE('UTC', tz.zone_name, oir.returned_at)) as report_date,
        oir.office_id,
        oir.product_id,
        
        SUM(oir.returned_amount) as refunded_amount
    
    FROM refunded_items_with_details oir

    INNER JOIN customers c 
        ON oir.comp_id = c.comp_id

    INNER JOIN timezone tz 
        ON c.timezone_id = tz.id

    GROUP BY 1, 2, 3

),

final as (

    SELECT
        order_items_daily.report_date,

        SUM(gross_receipts) as total_gross_receipts,
        SUM(refunded_amount) as refunded_amount

    FROM order_items_daily

    LEFT JOIN refunded_items_daily
        ON order_items_daily.report_date = order_items_daily.report_date
        AND order_items_daily.office_id = order_items_daily.office_id
        AND order_items_daily.product_id = order_items_daily.product_id

    GROUP BY 1
    
)

SELECT * FROM final



