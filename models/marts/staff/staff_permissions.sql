{{
    config(
        materialized='table',
        sort=['comp_id', 'user_id', 'permission_name'],
        dist='user_id'
    )
}}


with permissions_dim as (

    SELECT 
        id as permission_id, 
        name as permission_name, 
        description as indica_permission_name,
        zone as permission_category
    FROM {{ ref('stg_io__sf_guard_permission') }}

),

users as (

    SELECT 
        comp_id, 
        id as user_id, 
        first_name, 
        last_name 
    FROM {{ ref('stg_io__sf_guard_user') }}
    WHERE deleted_at IS NULL

),

cross_joined as (

    SELECT *
    FROM permissions_dim
    CROSS JOIN users

),

user_permission_mapping as (

    SELECT 
        comp_id, 
        user_id, 
        permission_id
    FROM {{ ref('stg_io__sf_guard_user_permission') }}

),

mapped as (

    SELECT 
        cross_joined.comp_id, 
        cross_joined.user_id,
        cross_joined.first_name,
        cross_joined.last_name,
        cross_joined.permission_id,
        cross_joined.permission_name,
        cross_joined.indica_permission_name,
        cross_joined.permission_category,
        user_permission_mapping.permission_id IS NOT NULL as is_permitted

    FROM cross_joined
    
    LEFT JOIN user_permission_mapping
        ON cross_joined.comp_id = user_permission_mapping.comp_id
        AND cross_joined.user_id = user_permission_mapping.user_id
        AND cross_joined.permission_id = user_permission_mapping.permission_id

),

user_group_mapping as (

    SELECT * FROM {{ ref('stg_io__sf_guard_user_group') }}

),

user_groups as (

    SELECT * FROM {{ ref('stg_io__sf_guard_group') }}

),

user_groups_by_user as (

    SELECT 
        user_group_mapping.comp_id,
        user_group_mapping.user_id,
        LISTAGG(DISTINCT user_groups.name, ', ') as user_group_list

    FROM user_group_mapping

    INNER JOIN user_groups
        ON user_group_mapping.group_id = user_groups.id
        AND user_group_mapping.comp_id = user_groups.comp_id
    
    GROUP BY 1, 2

),

customers as (

    SELECT * FROM {{ ref('int_customers') }}

),

final as (

    SELECT
        mapped.comp_id,
        customers.domain_prefix,
        mapped.user_id,
        mapped.first_name,
        mapped.last_name,
        user_groups_by_user.user_group_list,
        mapped.permission_id,
        mapped.permission_name,
        mapped.indica_permission_name, 
        mapped.permission_category,
        mapped.is_permitted

    FROM mapped

    INNER JOIN customers
        ON mapped.comp_id = customers.comp_id

    LEFT JOIN user_groups_by_user
        ON mapped.comp_id = user_groups_by_user.comp_id
        AND mapped.user_id = user_groups_by_user.user_id


)

SELECT * FROM final