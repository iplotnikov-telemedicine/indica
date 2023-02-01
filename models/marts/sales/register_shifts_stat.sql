
{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key=['comp_id', 'register_id', 'open_at'],
        sort=['comp_id', 'register_id'],
        dist='open_at'
    )
}}


with register_log as (

    select * 
    from {{ ref('stg_io__register_log') }}
    
    WHERE 1 = 1
    {% if is_incremental() %}
        and created_at > (select max(open_at) from {{ this }})
    {% endif %}

),

service_history as (

    select * 
    from {{ ref('stg_io__service_history') }}
    WHERE 1 = 1
    {% if is_incremental() %}
        and created_at > (select max(open_at) from {{ this }})
    {% endif %}

),

orders as (

    select * 
    from {{ ref('stg_io__warehouse_orders') }}
    WHERE 1 = 1
    {% if is_incremental() %}
        and created_at > (select max(open_at) from {{ this }})
    {% endif %}

),

register_sales as (

    SELECT
        register_log.comp_id,
        register_log.register_id,
        register_log.created_at,
        orders.id as order_id,
        orders.total_amount,
        orders.order_status,
        orders.type as order_type,
        datediff(minute, orders.placed_at, orders.confirmed_at) as exec_order_minutes

    FROM register_log

    INNER JOIN service_history
        ON register_log.comp_id = service_history.comp_id
        AND register_log.service_history_id = service_history.id
    
    INNER JOIN orders
        ON service_history.comp_id = orders.comp_id
	    AND service_history.order_id = orders.id

    WHERE register_log.type = 3 
        and orders.order_status in ('completed', 'canceled')
        and orders.type in ('walkin', 'pickup', 'delivery')

),

shifts as (

    SELECT * FROM {{ ref('int_register_shifts') }}

    WHERE 1 = 1
    {% if is_incremental() %}
        and open_at > (select max(open_at) from {{ this }})
    {% endif %}

),

customers as (

    select * from {{ ref('int_customers') }}

),

registers as (

    select * from {{ ref('stg_io__registers') }}

),

grouped as (

    SELECT 
        shifts.comp_id,
        shifts.register_id,
        shifts.register_name,
        shifts.open_at,
        shifts.closed_at,
        register_sales.order_type,
        register_sales.order_status,

        COUNT(register_sales.order_id) as orders_count,
        COALESCE(SUM(register_sales.total_amount), 0) as sales_amount,
        COALESCE(SUM(register_sales.exec_order_minutes), 0) as wait_minutes

    FROM shifts

    INNER JOIN register_sales
        ON shifts.comp_id = register_sales.comp_id
        AND shifts.register_id = register_sales.register_id
        AND register_sales.created_at >= shifts.open_at
        AND register_sales.created_at <= shifts.closed_at

    GROUP BY 1, 2, 3, 4, 5, 6, 7

),

final as (

    SELECT 
        grouped.comp_id,
        customers.domain_prefix,
        grouped.register_id,
        grouped.register_name,
        grouped.open_at,
        grouped.closed_at,
        grouped.order_type,
        grouped.order_status,
        grouped.orders_count,
        grouped.sales_amount,
        grouped.wait_minutes,
        registers.office_id

    FROM grouped

    INNER JOIN registers
        ON grouped.comp_id = registers.comp_id
        AND grouped.register_id = registers.id

    INNER JOIN customers
        ON grouped.comp_id = customers.comp_id

)

SELECT * FROM final

