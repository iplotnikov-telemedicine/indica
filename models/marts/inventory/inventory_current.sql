{{
    config(
        materialized='table',
        unique_key=['office_id', 'product_id'],
        sort=['comp_id'],
        dist='product_id'
    )
}}

with poq as (
    select * from {{ ref('inventory_poq') }}
    WHERE date = (select max(date) from {{ ref('inventory_poq') }})
),

actions_dictionary as (
    select distinct comp_id, office_id, product_id
    from {{ ref('actions_dictionary') }}
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
        a.comp_id,
        companies.domain_prefix,
        a.office_id,
        offices.office_name,
        a.product_id,
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
        q.inventory_poq as inventory_current
    FROM actions_dictionary a
    LEFT JOIN poq q
        on a.comp_id = q.comp_id
        and a.office_id = q.office_id
        and a.product_id = q.product_id
    LEFT JOIN companies 
        on a.comp_id = companies.comp_id
    LEFT JOIN offices
        on a.office_id = offices.office_id
    LEFT JOIN products_with_details p 
        on a.comp_id = p.comp_id
        and a.product_id = p.prod_id
)

SELECT * FROM final
