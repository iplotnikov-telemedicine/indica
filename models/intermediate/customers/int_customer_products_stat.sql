
{{ config(materialized='ephemeral') }}


with products as (

    select * from {{ ref('stg_io__products')}}

),

final as (

    select

        comp_id,
        COUNT(*) as total_products_count,
        COUNT(CASE is_marijuana_product WHEN 1 THEN 1 END) as mj_products_count,
        total_products_count - mj_products_count as non_mj_products_count

    from products

    group by 1

)

select * from final