
{{
    config(
        materialized='table',
        sort=['comp_id', 'patient_id'],
        dist='patient_id'
    )
}}


with warehouse_orders as (

    select * from {{ ref('stg_io__warehouse_orders') }}

),

int_patients_top_brands as (

    select * from {{ ref('int_patients_top_brands') }}

),

orders as (

	select
		comp_id,
		patient_id,
		id as order_id,
		marketplace,
		placed_at,
		lag(placed_at) over (partition by comp_id, patient_id order by placed_at) as lag_placed_at,
		datediff(day, cast(lag_placed_at as date), cast(placed_at as date)) + 1 as days_between_orders,
		total_amount,
		office_id,
        sum_discount
	from warehouse_orders wo
	where confirmed_at is not null

),

final as (

    select
        orders.comp_id,
        orders.patient_id,
        int_patients_top_brands.top_purchased_brands,
        count(CASE WHEN sum_discount > 0 THEN 1 END) as discounted_orders_count,
        count(order_id) as orders_count,
        sum(total_amount) as total_purchase_amount,
        sum(case when marketplace in (1, 4) then total_amount end) as offline_purchase_amount,
        total_purchase_amount - offline_purchase_amount as online_purchase_amount,
        min(placed_at) as min_order_dt,
        max(placed_at) as max_order_dt,
        -- datediff(day, cast(max_order_dt as date), cast(now() as date)) + 1 as last_order_days_ago,
        max(days_between_orders) as max_silent_period_in_days,
        max(total_amount) as max_order_amount,
        avg(total_amount) as avg_order_amount,
        count(distinct office_id) as distinct_offices_count,
        count(order_id) as total_order_count,
        avg(days_between_orders) as avg_order_freq_in_days,
        count(case when marketplace in (1, 4) then total_amount end) as offline_order_count,
        total_order_count - offline_order_count as online_order_count,
        count(distinct case when marketplace in (1, 4) then office_id end) as visited_offices_count,
        count(case DATE_PART(dayofweek, placed_at) when 0 then order_id end) as sunday_order_count,
        count(case DATE_PART(dayofweek, placed_at) when 1 then order_id end) as monday_order_count,
        count(case DATE_PART(dayofweek, placed_at) when 2 then order_id end) as tuesday_order_count,
        count(case DATE_PART(dayofweek, placed_at) when 3 then order_id end) as wednesday_order_count,
        count(case DATE_PART(dayofweek, placed_at) when 4 then order_id end) as thursday_order_count,
        count(case DATE_PART(dayofweek, placed_at) when 5 then order_id end) as friday_order_count,
        count(case DATE_PART(dayofweek, placed_at) when 6 then order_id end) as saturday_order_count
    from orders
    left join int_patients_top_brands
        on orders.patient_id = int_patients_top_brands.patient_id
        and orders.comp_id = int_patients_top_brands.comp_id
    group by 1, 2, 3

)

select * from final