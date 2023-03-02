{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key=['comp_id', 'order_id', 'id'],
        sort=['comp_id', 'order_id', 'id'],
        dist='created_at'
    )
}}


select
	wol.comp_id,
	wol.id,
	wol.order_id,
	wol."type",
	t.type_name,
	wol.sf_guard_user_id,
	wol.sf_guard_user_name,
	wol.created_at,
	--max(wol.created_at) over (partition by wol.comp_id, wol.order_id, wol."type") as max_order_timestamp,
	row_number() over (partition by wol.comp_id, wol.order_id, wol."type" order by wol.created_at desc) as rn
from
	{{ ref('stg_io__warehouse_order_logs') }} wol
left join {{ ref('order_log_types') }} t on t.type_id = wol.type
{% if is_incremental() %}
	where created_at > (select max(max_order_timestamp) from {{ this }})
{% endif %}