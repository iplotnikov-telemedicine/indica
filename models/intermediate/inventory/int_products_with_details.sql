
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


final as (

    SELECT
        products.comp_id,
        products.prod_id,
        products.prod_name,
        products.prod_price as prod_cost,
        products.prod_sku,
        products.prod_price_type as unit,
        products.net_weight,
        products.brand_id,
        
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