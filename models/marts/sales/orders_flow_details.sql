{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key=['comp_id', 'order_id'],
        sort=['comp_id', 'order_id'],
        dist='max_order_timestamp'
    )
}}


with wo_duplicates as (
	select
		comp_id,
		id,
		order_id,
		"type",
		sf_guard_user_id,
		sf_guard_user_name,
		created_at,
		max(created_at) over (partition by comp_id, order_id, "type") as max_order_timestamp,
		row_number() over (partition by comp_id, order_id, "type" order by created_at desc) as rn
	from
		{{ ref('stg_io__warehouse_order_logs') }}
	{% if is_incremental() %}
		where created_at > (select max(max_order_timestamp) from {{ this }})
	{% endif %}
)
select 
	comp_id,
	order_id,
	max(max_order_timestamp) as max_order_timestamp,
	max(case type when 0 then created_at end) as init_at,
	max(case type when 0 then sf_guard_user_name end) as init_by,
	max(case type when 0 then sf_guard_user_id end) as init_by_id,
	max(case type when 1 then created_at end) as pending_at,
	max(case type when 1 then sf_guard_user_name end) as pending_by,
	max(case type when 1 then sf_guard_user_id end) as pending_by_id,
	max(case type when 2 then created_at end) as accepted_at,
	max(case type when 2 then sf_guard_user_name end) as accepted_by,
	max(case type when 2 then sf_guard_user_id end) as accepted_by_id,
	max(case type when 3 then created_at end) as declined_at,
	max(case type when 3 then sf_guard_user_name end) as declined_by,
	max(case type when 3 then sf_guard_user_id end) as declined_by_id,
	max(case type when 4 then created_at end) as assignet_at,
	max(case type when 4 then sf_guard_user_name end) as assignet_by,
	max(case type when 4 then sf_guard_user_id end) as assignet_by_id,
	max(case type when 5 then created_at end) as not_home_at,
	max(case type when 5 then sf_guard_user_name end) as not_home_by,
	max(case type when 5 then sf_guard_user_id end) as not_home_by_id,
	max(case type when 6 then created_at end) as canceled_at,
	max(case type when 6 then sf_guard_user_name end) as canceled_by,
	max(case type when 6 then sf_guard_user_id end) as canceled_by_id,
	max(case type when 7 then created_at end) as delivered_at,
	max(case type when 7 then sf_guard_user_name end) as delivered_by,
	max(case type when 7 then sf_guard_user_id end) as delivered_by_id,
	max(case type when 8 then created_at end) as completed_at,
	max(case type when 8 then sf_guard_user_name end) as completed_by,
	max(case type when 8 then sf_guard_user_id end) as completed_by_id,
	max(case type when 9 then created_at end) as returned_at,
	max(case type when 9 then sf_guard_user_name end) as returned_by,
	max(case type when 9 then sf_guard_user_id end) as returned_by_id,
	max(case type when 10 then created_at end) as reassignet_at,
	max(case type when 10 then sf_guard_user_name end) as reassignet_by,
	max(case type when 10 then sf_guard_user_id end) as reassignet_by_id,
	max(case type when 11 then created_at end) as fullfiling_started_at,
	max(case type when 11 then sf_guard_user_name end) as fullfiling_started_by,
	max(case type when 11 then sf_guard_user_id end) as fullfiling_started_by_id,
	max(case type when 12 then created_at end) as fullfiling_stopped_at,
	max(case type when 12 then sf_guard_user_name end) as fullfiling_stopped_by,
	max(case type when 12 then sf_guard_user_id end) as fullfiling_stopped_by_id,
	max(case type when 13 then created_at end) as fulfilled_at,
	max(case type when 13 then sf_guard_user_name end) as fulfilled_by,
	max(case type when 13 then sf_guard_user_id end) as fulfilled_by_id,
	max(case type when 14 then created_at end) as refunded_at,
	max(case type when 14 then sf_guard_user_name end) as refunded_by,
	max(case type when 14 then sf_guard_user_id end) as refunded_by_id,
	max(case type when 15 then created_at end) as shipped_at,
	max(case type when 15 then sf_guard_user_name end) as shipped_by,
	max(case type when 15 then sf_guard_user_id end) as shipped_by_id,
	max(case type when 16 then created_at end) as fulfilling_timeout_at,
	max(case type when 16 then sf_guard_user_name end) as fulfilling_timeout_by,
	max(case type when 16 then sf_guard_user_id end) as fulfilling_timeout_by_id,
	max(case type when 19 then created_at end) as delivery_started_at,
	max(case type when 19 then sf_guard_user_name end) as delivery_started_by,
	max(case type when 19 then sf_guard_user_id end) as delivery_started_by_id,
	max(case type when 20 then created_at end) as delivery_stopped_at,
	max(case type when 20 then sf_guard_user_name end) as delivery_stopped_by,
	max(case type when 20 then sf_guard_user_id end) as delivery_stopped_by_id,
	max(case type when 21 then created_at end) as rejected_at,
	max(case type when 21 then sf_guard_user_name end) as rejected_by,
	max(case type when 21 then sf_guard_user_id end) as rejected_by_id,
	max(case type when 39 then created_at end) as delivery_accepted_at,
	max(case type when 39 then sf_guard_user_name end) as delivery_accepted_by,
	max(case type when 39 then sf_guard_user_id end) as delivery_accepted_by_id,
	max(case type when 43 then created_at end) as exchanged_at,
	max(case type when 43 then sf_guard_user_name end) as exchanged_by,
	max(case type when 43 then sf_guard_user_id end) as exchanged_by_id
from wo_duplicates
where rn = 1
group by 1, 2