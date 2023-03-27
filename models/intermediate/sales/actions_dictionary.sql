{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key=['office_id', 'product_id', 'ctz_date'],
        sort=['office_id', 'product_id'],
        dist='ctz_date'
    )
}}

with actions_dictionary as (
    select 
        coalesce(i.comp_id, t.comp_id) as comp_id,
        coalesce(i.office_id, t.office_id) as office_id,
        coalesce(i.product_id, t.product_id) as product_id,
        coalesce(i.date, t.report_date) as ctz_date
    from test.inventory_daily i
    full join test.transactions_daily t 
        on i.office_id = t.office_id
        and i.product_id = t.product_id
        and i.date = t.report_date
    where coalesce(i.date, t.report_date) >= '2022-01-01'::date
        {% if is_incremental() %}
            and coalesce(i.date, t.report_date) > (select max(ctz_date) from {{ this }})
            or coalesce(i.comp_id, t.comp_id) not in (select distinct comp_id from {{ this }})
        {% endif %}
)

select * from test.units_of_weight
