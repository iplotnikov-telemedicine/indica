
{{
    config(
        materialized='incremental',
        unique_key=['office_id', 'report_date', 'channel_name', 'type', 'payment_method'],
        sort=['office_id', 'report_date'],
        dist='report_date'
    )
}}


{% set payment_methods = {
    'method1_amount':'cash', 
    'method2_amount':'credit card',
    'method3_amount':'check',
    'method4_amount':'CC terminal',
    'method5_amount':'bonus',
    'method6_amount':'Potify bonus',
    'method7_amount':'DC terminal'
} %}


with warehouse_orders as (

    select * from {{ ref('stg_io__warehouse_orders') }}

),

grouped as (

    select

        comp_id,
        office_id,
        DATE(confirmed_at) as report_date,
        marketplace as channel_id,
        type,
        CASE WHEN method1_amount > 0 THEN 1 ELSE 0 END
            + CASE WHEN method2_amount > 0 THEN 1 ELSE 0 END
            + CASE WHEN method3_amount > 0 THEN 1 ELSE 0 END
            + CASE WHEN method4_amount > 0 THEN 1 ELSE 0 END
            + CASE WHEN method5_amount > 0 THEN 1 ELSE 0 END
            + CASE WHEN method6_amount > 0 THEN 1 ELSE 0 END
            + CASE WHEN method7_amount > 0 THEN 1 ELSE 0 END as number_of_methods_used,
        sum(method1_amount) as method1_amount,
        sum(method2_amount) as method2_amount, 
        sum(method3_amount) as method3_amount, 
        sum(method4_amount) as method4_amount, 
        sum(method5_amount) as method5_amount, 
        sum(method6_amount) as method6_amount, 
        sum(method7_amount) as method7_amount,
        count(id) as order_quantity

    from warehouse_orders

    {% if is_incremental() %}
        where confirmed_at > (select max(report_date) from {{ this }})
    {% endif %}

    group by 1, 2, 3, 4, 5, 6

),

channels as (
    select * from {{ ref('channels') }}
),

customers as (
    select * from {{ ref('int_customers') }}
),


unioned as (

    select 

        comp_id,
        office_id,
        report_date,
        channel_id,
        type,
        'mixed' as payment_method,
        (method1_amount 
            + method2_amount 
            + method3_amount 
            + method4_amount 
            + method5_amount 
            + method6_amount 
            + method7_amount) as sales_volume,
        order_quantity
        
    FROM grouped
    where number_of_methods_used > 1


    {% for field_name, payment_method in payment_methods.items() %}

        UNION ALL

        select

            comp_id,
            office_id,
            report_date,
            channel_id,
            type,
            '{{ payment_method }}' as payment_method,
            {{ field_name }} as sales_volume,
            order_quantity

        from grouped
        where {{ field_name }} > 0 and number_of_methods_used = 1
            
    {% endfor %}

),

final as (

    select
        customers.domain_prefix,
        unioned.comp_id,
        unioned.office_id,
        unioned.report_date,
        channels.channel_name,
        unioned.type,
        unioned.payment_method,
        unioned.sales_volume,
        unioned.order_quantity
    from unioned
    inner join customers
        on unioned.comp_id = customers.comp_id
    left join channels
        on unioned.channel_id = channels.channel_id

)

select * from final

