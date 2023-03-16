{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key=['comp_id', 'order_id', 'type'],
        sort=['comp_id', 'order_id', 'type'],
        dist='created_at'
    )
}}

with
{% if is_incremental() %}
tmp as (
	select comp_id, order_id, "type", max(created_at) as max_created_at
	from {{ this }}
	where created_at > dateadd(day,-30,CURRENT_DATE)
	group by 1,2,3
),
{% endif %}

wol_with_rn as (
select
	wol.comp_id,
	wol.id,
	wol.order_id,
	wol."type",
	lt.type_name,
	wol.sf_guard_user_id,
	wol.sf_guard_user_name,
	wol.created_at,
	row_number() over (partition by wol.comp_id, wol.order_id, wol."type" order by wol.created_at desc) as rn
from
	{{ ref('stg_io__warehouse_order_logs') }} wol
left join {{ ref('order_log_types') }} lt on lt.type_id = wol.type
{% if is_incremental() %}
left join tmp t on wol.comp_id = t.comp_id and wol.order_id = t.order_id and wol.type = t.type
where true
	and wol.created_at >= CURRENT_DATE - INTERVAL '16 HOUR'
	and wol.created_at < CURRENT_DATE + INTERVAL '8 HOUR'
	and (
		(wol.created_at > t.max_created_at and wol.comp_id = t.comp_id and wol.order_id = t.order_id and wol.type = t.type)
		or wol.comp_id || '_' || wol.order_id || '_' || wol."type" not in (select comp_id || '_' || order_id || '_' || "type" from tmp)
		)
{% endif %}
)
select * from wol_with_rn where rn = 1