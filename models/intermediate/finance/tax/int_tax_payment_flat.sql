
{{ config(materialized='ephemeral') }}


with tax_payment_unioned as (
    
    {% for tax_type in ['state_sales_tax', 'city_sales_tax', 'city_local_tax', 'excise_tax'] %}
    
        SELECT
            comp_id,
            order_id,
            '{{ tax_type }}' as tax_type,
            sum({{ tax_type }}) as tax_amount

        FROM {{ ref('stg_io__tax_payment') }}
        WHERE {{ tax_type }} > 0
        
        {% if is_incremental() %}
            and updated_at > (select max(updated_at) from {{ this }})
            or comp_id not in (select distinct comp_id from {{ this }})
        {% endif %}

        GROUP BY 1, 2, 3
    
        {% if not loop.last %}
            UNION ALL
        {% endif %}

    {% endfor %}

)

select * from tax_payment_unioned