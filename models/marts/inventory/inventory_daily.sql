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
)

select * from inventory_poq
union all
select * from inventory_history
