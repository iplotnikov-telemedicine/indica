
{{ config(materialized='view') }}


SELECT *
FROM {{ ref('stg_io__companies') }}
WHERE 1=1
    and (db_name like '%_company'
        and is_blank = 0
        and project_type = 'indica'
        and not domain_prefix like '%prod'
        and not domain_prefix like 'test%'
        and not domain_prefix like '%demo%'
        and not comp_email like '%maildrop.cc'
        and not comp_email like '%indica%'
        and not comp_name like 'Blank company%'
        and not comp_name like '%test%'
        and not comp_name like '%Test%'
        and not comp_name like '%xxxx%'
        and plan <> 5
        and comp_id not in (8580, 724, 6805, 8581, 6934, 8584, 
            8585, 3324, 8582, 6022, 3439, 8583, 8586, 6443, 8588, 
            6483, 7900, 8587, 8589, 9471, 7304, 7523, 8911, 213
        )
        and comp_is_approved = 1
    )
    or (comp_id in (10461, 9868))
ORDER BY comp_id