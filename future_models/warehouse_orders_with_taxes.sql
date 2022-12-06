
{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert'
    )
}}


with warehouse_orders as (

    SELECT * FROM {{ ref('stg3_warehouse_orders') }}

),

tax_payment_flat as (

    SELECT * FROM {{ ref('tax_payment_flat') }}

),

warehouse_orders_joined as (

    SELECT 
        warehouse_orders.*,
        {{ dbt_utils.star(from=ref('tax_payment_flat'), except=["order_id"]) }}

    FROM warehouse_orders

    LEFT JOIN tax_payment_flat
        ON warehouse_orders.comp_id = tax_payment_flat.comp_id
        AND warehouse_orders.id = tax_payment_flat.order_id

)

select * from tax_payment_unioned
