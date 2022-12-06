
{{ config(materialized='ephemeral') }}


with warehouse_orders as (

    select * from {{ ref('stg_io__warehouse_orders') }}

),

warehouse_order_items as (

    select * from {{ ref('stg_io__warehouse_order_items') }}

),

products as (

    select * from {{ ref('stg_io__products') }}

),

brands as (

    select * from {{ ref('stg_io__brands') }}

),

brand_ranking as (

	select 
		wo.comp_id,
		wo.patient_id, 
		coalesce(b.brand_name, 'Unknown') as brand_name, 
		sum(woi.total_amount) as total_amount_per_pat, 
		rank() over (partition by wo.comp_id, wo.patient_id order by total_amount_per_pat desc) as brand_rank
	from warehouse_orders wo
	inner join warehouse_order_items woi 
		on wo.id = woi.order_id
		and wo.comp_id = woi.comp_id 
	left join products p 
		on woi.product_id = p.prod_id
		and wo.comp_id = p.comp_id 
	left join brands b 
		on p.brand_id = b.id
		and p.comp_id = b.comp_id 
	group by 1, 2, 3

),

final as (
    select 
        comp_id,
        patient_id,
        listagg(brand_name, ', ') within group (order by total_amount_per_pat desc) as top_purchased_brands
    from brand_ranking
    where brand_rank <= 3
    group by 1, 2
)

select * from final