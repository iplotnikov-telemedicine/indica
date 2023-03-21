{{
    config(
        materialized='table',
        unique_key=['office_id', 'product_id'],
        sort=['comp_id'],
        dist='product_id'
    )
}}

with poq as (
    select comp_id, poq_office_id, poq_prod_id, sum(poq_qty) as poq_qty
    from {{ ref('stg_io__product_office_qty') }}
    group by 1,2,3
),

companies as (
    select * from {{ ref('stg_io__companies') }}
),

products_with_details as (
    select * from {{ ref('int_products_with_details') }}
),

offices as (
    select * from {{ ref('stg_io__offices') }}
),

final as (
    SELECT
        poq.comp_id,
        companies.domain_prefix,
        poq.poq_office_id as office_id,
        offices.office_name,
        poq.poq_prod_id as product_id,
        p.prod_name,
        p.unit,
        p.prod_cost,
        p.brand_id,
        p.brand_name,
        p.vendor_name,
        p.direct_category,
        p.parent_category,
        p.sub_category_1,
        p.sub_category_2,
        poq.poq_qty as inventory_current
    FROM poq
    LEFT JOIN companies 
        on poq.comp_id = companies.comp_id
    LEFT JOIN offices
        on poq.poq_office_id = offices.office_id
    LEFT JOIN products_with_details p 
        on poq.comp_id = p.comp_id
        and poq.poq_prod_id = p.prod_id
)

SELECT * FROM final
