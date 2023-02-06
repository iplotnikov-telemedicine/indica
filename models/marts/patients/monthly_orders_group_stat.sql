
{{
    config(
        materialized='table',
        sort=['comp_id', 'report_month'],
        dist='report_month'
    )
}}


WITH warehouse_orders AS (

    select * from {{ ref('stg_io__warehouse_orders') }}

),

warehouse_order_items AS (

    select * from {{ ref('stg_io__warehouse_order_items') }}

),

products AS (

    select * from {{ ref('stg_io__products') }}

),

customers AS (

    select * from {{ ref('int_customers') }}

),

timezone AS (

    select * from {{ ref('stg_io__timezone') }}

),


monthly_stat AS (

    SELECT

        DATE_TRUNC('month', CONVERT_TIMEZONE('UTC', tz.zone_name, wo.confirmed_at)) as report_month,
        wo.comp_id,
        c.domain_prefix,
        wo.patient_id,

        COUNT(DISTINCT wo.id) AS patient_orders,
        COUNT(DISTINCT pr.prod_id) AS unique_products,
        COUNT(DISTINCT pr.prod_category_id) AS unique_categories,
        COUNT(DISTINCT pr.brand_id) AS unique_brands,
        SUM(woi.qty + woi.qty_free) AS order_items,
        SUM(woi.paid_amount - woi.tax -
            CASE WHEN woi.paid_amount IS NOT NULL AND woi.paid_amount <> 0
            THEN woi.returned_amount - (woi.returned_amount * woi.tax / woi.paid_amount)
            ELSE 0 END) AS order_net_sales

    FROM warehouse_orders wo

    INNER JOIN customers c 
        ON wo.comp_id = c.comp_id

    INNER JOIN timezone tz 
        ON c.timezone_id = tz.id

    INNER JOIN warehouse_order_items woi
        ON wo.id = woi.order_id 
        AND wo.comp_id = woi.comp_id

    INNER JOIN products pr
        ON woi.product_id = pr.prod_id
        AND woi.comp_id = pr.comp_id

    WHERE wo.patient_id IS NOT NULL and wo.confirmed_at IS NOT NULL
    
    GROUP BY 1, 2, 3, 4

),

final as (

    SELECT
        report_month,
        comp_id,
        domain_prefix,
        patient_orders as patient_orders_group,

        COUNT(patient_id) AS patients,
        SUM(patient_orders) AS orders,
        AVG(unique_products) AS unique_products,
        AVG(unique_categories) AS unique_categories,
        AVG(unique_brands) AS unique_brands,
        SUM(order_items) AS order_items,
        SUM(order_net_sales) AS order_net_sales

    FROM monthly_stat

    GROUP BY 1, 2, 3, 4

)


select * from final