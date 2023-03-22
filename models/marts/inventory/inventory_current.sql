{{
    config(
        materialized='table',
        unique_key=['office_id', 'product_id'],
        sort=['comp_id'],
        dist='product_id'
    )
}}


with units as (
    select * from {{ ref('units_of_weight') }}
),

poq as (
    select * from {{ ref('stg_io__product_office_qty') }}
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

grouped as (
    select 
        p.comp_id,
        p.poq_office_id,
        p.poq_prod_id,
        sum(p.poq_qty * coalesce(u.grams, 1.0)) as poq_qty_grams
    from poq p
    left join units u on p.poq_item_type = u.unit
    group by 1,2,3
),

final as (
    SELECT
        g.comp_id,
        companies.domain_prefix,
        g.poq_office_id as office_id,
        offices.office_name,
        g.poq_prod_id as product_id,
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
        g.poq_qty_grams as inventory_current
    FROM grouped g
    INNER JOIN companies 
        on g.comp_id = companies.comp_id
    INNER JOIN offices
        on g.poq_office_id = offices.office_id
    INNER JOIN products_with_details p 
        on g.comp_id = p.comp_id
        and g.poq_prod_id = p.prod_id
)

SELECT * FROM final
