
{{ config(materialized='ephemeral') }}


with patients as (

    select * from {{ ref('stg_io__patients')}}

),

final as (

    select

        comp_id,
        COUNT(*) as total_patients_count,
        COUNT(CASE pat_gender WHEN 'Male' THEN 1 END) as male_patients_count,
        COUNT(CASE pat_gender WHEN 'Female' THEN 1 END) as female_patients_count,
        total_patients_count - male_patients_count - female_patients_count as unspecified_patients_count

    from patients

    group by 1

)

select * from final