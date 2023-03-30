{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key=['comp_id', 'date', 'product_id', 'office_id'],
        sort=['comp_id', 'date'],
        dist='date'
    )
}}

with customers as (
    select * from {{ ref('int_customers') }}
),

timezone as (
    select * from {{ ref('stg_io__timezone') }}
),

units as (
    select * from {{ ref('units_of_weight') }}
),

snapshots as (
    select * from {{ ref('product_office_qty_snapshot') }}
    where poq_qty > 0
),

dates as (
    select date_day, date_day + interval '1 day' as day_end
    from {{ ref('util_dates') }}
    where date_day >= '2023-02-13'::datetime 
        and date_day < current_date::datetime
    {% if is_incremental() %}
        and date_day > (select max(date)::datetime from {{ this }})
    {% endif %}
),

convert_tz_units as (
    select s.*, 
        CONVERT_TIMEZONE('UTC', t.zone_name, dbt_valid_from) as dbt_valid_from_tz,
        CONVERT_TIMEZONE('UTC', t.zone_name, dbt_valid_to) as dbt_valid_to_tz,
        s.poq_qty * coalesce(u.grams, 1.0) as poq_qty_grams
    from snapshots s
    inner join customers c on s.comp_id = c.comp_id
    inner join timezone t on c.timezone_id = t.id
    left join units u on s.poq_item_type = u.unit
),

join_date as (
    select 
        comp_id,
        date_day::date as date,
        poq_prod_id as product_id,
        poq_office_id as office_id,
        sum(poq_qty_grams) as inventory_poq
    from convert_tz_units
    inner join dates 
        on coalesce(dbt_valid_from_tz, current_date::datetime - interval '1 day') < day_end
        and coalesce(dbt_valid_to_tz, current_date::datetime + interval '1 day') > day_end 
    where day_end is not null
    group by 1, 2, 3, 4
)

select * from join_date
