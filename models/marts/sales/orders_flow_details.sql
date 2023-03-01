{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key=['comp_id', 'order_id'],
        sort=['comp_id', 'order_id'],
        dist='max_order_timestamp'
    )
}}


{% set wol_types = [
    "init",
	"pending",
	"accepted",
	"declined",
	"assigned",
	"not_home",
	"canceled",
	"delivered",
	"completed",
	"returned",
	"reassigned",
	"fulfilling_started",
	"fulfilling_stopped",
	"fulfilled",
	"refunded",
	"shipped",
	"fulfilling_timeout",
	"delivery_started",
	"delivery_stopped",
	"rejected",
	"delivery_accepted",
	"exchanged"
] %}


with wo_duplicates as (
	select
		wol.comp_id,
		wol.id,
		wol.order_id,
		wol."type",
		t.type_name,
		wol.sf_guard_user_id,
		wol.sf_guard_user_name,
		wol.created_at,
		max(wol.created_at) over (partition by wol.comp_id, wol.order_id, wol."type") as max_order_timestamp,
		row_number() over (partition by wol.comp_id, wol.order_id, wol."type" order by wol.created_at desc) as rn
	from
		{{ ref('stg_io__warehouse_order_logs') }} wol
	left join {{ ref('order_log_types') }} t on t.type_id = wol.type
	{% if is_incremental() %}
		where created_at > (select max(max_order_timestamp) from {{ this }})
	{% endif %}
)
select 
	comp_id,
	order_id,
	{% for wol_type in wol_types %}
	max(case when type_name = '{{wol_type}}' then created_at end) as {{wol_type}}_at,
	max(case when type_name = '{{wol_type}}' then sf_guard_user_name end) as {{wol_type}}_by,
	max(case when type_name = '{{wol_type}}' then sf_guard_user_id end) as {{wol_type}}_by_id,
	{% if not loop.last %},{% endif %}
	{% endfor %}
	max(max_order_timestamp) as max_order_timestamp
from wo_duplicates
where rn = 1
group by 1, 2