
{{ config(materialized='table') }}


with settings as (

    select * from {{ ref('stg_io__company_config')}} 

),

numbers as (

    select generated_number from {{ ref('numbers') }}

),

parsed as (

    select
        comp_id,
        json_extract_path_text(value, 'stores', '0', 'store') as store_name,
        {% for store_type in var("store_types") %}
            json_extract_path_text(value, 'stores', '0', '{{ store_type }}') as {{ store_type }}_list,
            json_array_length({{ store_type }}_list, true) as {{ store_type }}_list_length  {% if not loop.last -%} , {%- endif %}
        {% endfor %}
    from settings
    where name = 'auto_store_inventory_report_settings'

),

unioned as (    

    {% for store_type in var("store_types") %}
        select
            comp_id,
            store_name,
            '{{ store_type }}' as store_type,
            json_extract_array_element_text({{ store_type }}_list, numbers.generated_number::int - 1, true)::int as office_id
        from parsed
        cross join numbers
        where numbers.generated_number <= {{ store_type }}_list_length
    {% if not loop.last -%} union all {%- endif %}
    {% endfor %}

)

select * from unioned
