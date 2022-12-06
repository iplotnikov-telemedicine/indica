
{{
    config(
        materialized='incremental',
        sort=['comp_id', 'limit_type'],
        dist='created_at'
    )
}}


with limits as (

    select * 
    from {{ ref('stg_io__company_limits_audit_log') }}
    {% if is_incremental() %}
        where created_at > (select max(created_at) from {{ this }})
    {% endif %}

),

parsed as (

        select
            company_id as comp_id,
            diff,
            created_at,
            {% for limit_type in var("limit_types") %}
                json_extract_path_text(diff, '{{ limit_type }}') as {{ limit_type }} {% if not loop.last -%} , {%- endif %}
            {% endfor %}
        from limits

),

unioned as (

    {% for limit_type in var("limit_types") %}
        SELECT
            comp_id,
            '{{ limit_type }}' as limit_type,
            created_at,
            json_extract_path_text({{ limit_type }}, 'old') as old_value,
            json_extract_path_text({{ limit_type }}, 'new') as new_value
        FROM parsed
        WHERE {{ limit_type }} IS NOT NULL AND {{ limit_type }} <> ''
    {% if not loop.last -%} union all {%- endif %}
    {% endfor %}

)

select * from unioned

