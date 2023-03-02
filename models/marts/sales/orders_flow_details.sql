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


select 
	comp_id,
	order_id,
	max(created_at) as max_order_timestamp,
	{% for wol_type in wol_types %}
	max(case when type_name = '{{wol_type}}' then created_at end) as {{wol_type}}_at,
	max(case when type_name = '{{wol_type}}' then sf_guard_user_name end) as {{wol_type}}_by,
	max(case when type_name = '{{wol_type}}' then sf_guard_user_id end) as {{wol_type}}_by_id
	{% if not loop.last %},{% endif %}
	{% endfor %}
from {{ ref('wo_duplicates') }}
where rn = 1
group by 1, 2