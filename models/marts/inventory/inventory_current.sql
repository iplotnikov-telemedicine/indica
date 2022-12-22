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

offices as (
    select * from {{ ref('stg_io__offices') }}
),

grouped as (
    SELECT
        comp_id,
        domain_prefix,
        office_id,
        product_id,
        sum(inventory_turnover) as inventory_current
    FROM inventory_daily
    GROUP BY 1, 2, 3, 4
),

final as (
    SELECT
        grouped.comp_id,
        grouped.domain_prefix,
        grouped.office_id,
        offices.office_name,
        grouped.product_id,
        p.prod_name,
        p.unit,
        p.brand_id,
        p.brand_name,
        p.vendor_name,
        p.direct_category,
        p.parent_category,
        p.sub_category_1,
        p.sub_category_2,
        grouped.inventory_current
    FROM grouped
    LEFT JOIN offices
        on offices.office_id = grouped.office_id
    LEFT JOIN products_with_details p 
        on grouped.comp_id = p.comp_id
        and grouped.product_id = p.prod_id
)

SELECT * FROM final


