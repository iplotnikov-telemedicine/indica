
{{
    config(
        materialized='table',
        unique_key=['comp_id', 'prod_id'],
        sort=['comp_id', 'prod_id'],
        dist='prod_id'
    )
}}


with products as (

    SELECT * 
    FROM {{ ref('stg_io__products') }}

    -- {% if is_incremental() %}
    --     where updated_at > (select max(updated_at) from {{ this }})
    -- {% endif %}

),

product_categories as (

    SELECT * FROM {{ ref('stg_io__product_categories') }}

),

brands as (

    select * from {{ ref('stg_io__brands') }}

),

vendors as (

    select * from {{ ref('stg_io__product_vendors') }}

),

strain_types as (

    select * from {{ ref('strain_types')}}

),

product_price_group as (

    select * from {{ ref('stg_io__product_price_group') }}

),

product_prices as (

    select * from {{ ref('stg_io__product_prices') }}

),

prices as (

    select
        product_price_group.comp_id,
        product_price_group.product_id,
        min(product_prices.price) as prod_min_price,
        max(product_prices.price) as prod_max_price
    
    FROM product_price_group

    INNER JOIN product_prices
        ON product_price_group.id = product_prices.price_group_id
        AND product_price_group.comp_id = product_prices.comp_id
        AND (product_prices.weight_type is NULL OR product_prices.weight_type = 'gram')
        AND (product_prices.range_from is NULL OR product_prices.range_from = 1)

    GROUP BY 1, 2

),

final as (

    SELECT
        products.comp_id,
        products.prod_id,
        products.prod_name,
        products.prod_price as prod_cost,
        prices.prod_min_price as prod_min_price,
        prices.prod_max_price as prod_max_price,
        products.prod_sku,
        products.twcc,
        products.prod_is_excise as is_excise,
        strain_types.strain_type as strain_type,
        CASE products.brand_product_strain_name WHEN '' THEN NULL ELSE products.brand_product_strain_name END as strain_name,
        products.prod_price_type as unit,
        products.net_weight,
        products.brand_id,
        products.prod_is_hidden as is_hidden,
        products.deleted_at,
        products.custom_cost,
        
        brands.brand_name,

        vendors.id as vendor_id,
        vendors.name as vendor_name,

        product_categories.name as direct_category,

        CASE product_categories.level 
            WHEN 1 THEN product_categories.name
            WHEN 2 THEN product_categories_1.name
            WHEN 3 THEN product_categories_2.name
            ELSE NULL
        END as parent_category,

        CASE product_categories.level 
            WHEN 1 THEN NULL
            WHEN 2 THEN product_categories.name
            WHEN 3 THEN product_categories_1.name
            ELSE NULL
        END as sub_category_1,

        CASE product_categories.level 
            WHEN 1 THEN NULL
            WHEN 2 THEN NULL
            WHEN 3 THEN product_categories.name
            ELSE NULL
        END as sub_category_2

    FROM products

    LEFT JOIN brands
        ON products.brand_id = brands.id
        AND products.comp_id = brands.comp_id

    LEFT JOIN vendors
        ON products.prod_vendor_id = vendors.id
        AND products.comp_id = vendors.comp_id

    LEFT JOIN strain_types
        ON products.strain = strain_types.strain_type_id

    LEFT JOIN prices
 	    ON products.prod_id = prices.product_id
        AND products.comp_id = prices.comp_id

    LEFT JOIN product_categories 
        ON products.prod_category_id = product_categories.id
        AND products.comp_id = product_categories.comp_id

    LEFT JOIN product_categories  AS product_categories_1 
        ON product_categories.rgt < product_categories_1.rgt
        AND product_categories.lft > product_categories_1.lft
        AND product_categories.level = product_categories_1.level + 1
        AND product_categories.comp_id = product_categories_1.comp_id
        
    LEFT JOIN product_categories  AS product_categories_2 
        ON product_categories_1.rgt < product_categories_2.rgt
        AND product_categories_1.lft > product_categories_2.lft
        AND product_categories_1.level = product_categories_2.level + 1
        AND product_categories_1.comp_id = product_categories_2.comp_id

)

SELECT * FROM final