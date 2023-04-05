{{
    config(
        materialized='view'
    )
}}

with inventory_poq as (
    select * from {{ ref('inventory_poq') }}
),

inventory_history as (
    select * from {{ ref('inventory_history') }}
),

union_datasets as (
    select * from inventory_poq
    union all
    select * from inventory_history
),

final as (
    select *,
        inventory_poq - nvl(lag(inventory_poq, 1) over (partition by office_id, product_id order by date),0) as turnover_poq
    from union_datasets
)

select * from final
