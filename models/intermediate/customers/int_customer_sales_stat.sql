
{{ config(materialized='ephemeral') }}


with daily_sales as (

    select * from {{ ref('daily_sales') }}

),

final as (

    select
    
        comp_id,
        min(report_date) as min_report_date,
        max(report_date) as max_report_date,
        ceiling(months_between(max_report_date, min_report_date)) as months_with_indica,
        count(distinct case channel_name when 'sweede' then office_id end) as sweede_sales_offices_count

    from daily_sales
    
    group by 1

)

select * from final