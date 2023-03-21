
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
        or company_id not in (select distinct comp_id from {{ this }})
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

unpivoted as (
  
    SELECT
        comp_id,
        limit_type,
        created_at,
        json_extract_path_text(json_value, 'old') as old_value,
        json_extract_path_text(json_value, 'new') as new_value
    FROM (
        SELECT *
        FROM parsed
    ) 
    UNPIVOT (
        json_value FOR limit_type IN (
            {% for limit_type in var("limit_types") %}
                {{ limit_type }} {% if not loop.last -%} , {%- endif %}
            {% endfor %}
        )
    )
    WHERE json_value <> ''

)

select * from unpivoted

