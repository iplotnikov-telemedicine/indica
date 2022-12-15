{{
    config(
        materialized='table',
        unique_key=['office_id', 'product_id'],
        sort=['comp_id'],
        dist='product_id'
    )
}}

with inventory_daily as (
    select * from {{ ref('inventory_daily') }}
),

products_with_details as (
    select * from {{ ref('int_products_with_details') }}
),

final as (
select
    i.comp_id,
    i.domain_prefix,
    i.office_id,
    i.product_id,
    p.brand_id,
    p.prod_name,
    p.brand_name,
    p.direct_category,
    p.parent_category,
    p.sub_category_1,
    p.sub_category_2,
    any_value(office_name) as office_name,
    sum(inventory_turnover) as inventory_current
from inventory_daily i
left join products_with_details p 
    on i.comp_id = p.comp_id
    and i.product_id = p.prod_id
group by 1,2,3,4,5,6,7,8,9,10,11
)

SELECT * FROM final


