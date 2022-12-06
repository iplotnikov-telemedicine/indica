
{{
    config(
        materialized='table'
    )
}}


with events as (
    
    select * from {{ ref('int_customer_limits_events')}}

),

customers as (

    select * from {{ ref('int_customers') }}

),

unpivoted_customers as (

    SELECT *
    FROM (
        SELECT 
            comp_id,
            domain_prefix, 
            {% for limit_type in var("limit_types") %}
                {{ limit_type }}::text,
            {% endfor %}
            created_at as comp_joined_at,
            potify_sync_entity_updated_at as comp_last_updated_at
        FROM customers
    ) 
    UNPIVOT INCLUDE NULLS (
        initial_value FOR limit_type IN (
            {% for limit_type in var("limit_types") %}
                {{ limit_type }} {% if not loop.last -%} , {%- endif %}
            {% endfor %}
        )
    )

),

joined as (

    select
        c.comp_id,
        c.domain_prefix,
        c.comp_joined_at,
        c.comp_last_updated_at,
        c.limit_type,
        c.initial_value,
        events.created_at,
        events.old_value,
        events.new_value,
        row_number() over (partition by c.comp_id, c.limit_type order by events.created_at) as rn,
        lead(events.created_at) over (partition by c.comp_id, c.limit_type order by events.created_at) as next_created_at
    from unpivoted_customers c
    left join events
        on events.comp_id = c.comp_id
        and events.limit_type = c.limit_type

),

final as (

    -- only limits that have not changed so far
    select
        comp_id,
        domain_prefix,
        limit_type,
        comp_joined_at as valid_from,
        comp_last_updated_at as valid_to,
        initial_value as value
    from joined
    where rn = 1 and old_value is null

    union all
    
    -- the very first changes of limits that have chanhed at least once
    select
        comp_id,
        domain_prefix,
        limit_type,
        comp_joined_at as valid_from,
        created_at as valid_to,
        old_value as value
    from joined
    where rn = 1 and old_value is not null

    union all   

    -- all the limit changes other than the very first ones
    select
        comp_id,
        domain_prefix,
        limit_type,
        created_at as valid_from,
        COALESCE(next_created_at, comp_last_updated_at) as valid_to,
        new_value as value
    from joined
    where old_value is not null

)

select * from final