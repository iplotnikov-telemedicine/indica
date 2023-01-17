
{{
    config(
        materialized='table',
        sort=['comp_id', 'patient_id'],
        dist='patient_id'
    )
}}


with patients as (

    select * from {{ ref('stg_io__patients')}}

),

patient_types as (

    select * from {{ ref('patient_types')}}

),

customers as (

    select * from {{ ref('int_customers') }}

),

int_patients_order_stat as (

    select * from {{ ref('int_patients_order_stat') }}

),

final as (
    select
	c.comp_id,
	c.domain_prefix,
	pat_id as patient_id,
	pat_first_name as first_name,
	pat_last_name as last_name,
	CASE WHEN pat_gender IN ('Male', 'M', 'male') THEN 'Male'  
        WHEN pat_gender IN ('Female', 'F', 'female') THEN 'Female'
        ELSE 'Unspecified' END as gender,
	pat_dob as date_of_birth,
	pat_city_name as city_name,
    c.state_name as state_name,
	pat_created_at_date as created_date,
	pat_last_visit_date as last_visit_date,
	pt.type_name as patient_type,
    p.phone_is_consented as has_phone_consent,
    p.email_is_consented as has_email_consent,
	top_purchased_brands, 
    total_purchase_amount, 
    offline_purchase_amount, 
	online_purchase_amount,
    discounted_orders_count,
    orders_count,
    min_order_dt, 
    max_order_dt,
	max_silent_period_in_days, 
    max_order_amount, 
    avg_order_amount, 
	distinct_offices_count, 
    total_order_count, 
    avg_order_freq_in_days, 
	offline_order_count, 
    online_order_count, 
	visited_offices_count, 
    sunday_order_count, 
    monday_order_count, 
	tuesday_order_count, 
    wednesday_order_count, 
    thursday_order_count, 
	friday_order_count, 
    saturday_order_count

from patients p

inner join patient_types pt
    on p.type = pt.type_id

inner join int_patients_order_stat pos 
	on p.pat_id = pos.patient_id
	and p.comp_id = pos.comp_id

inner join customers c 
	on p.comp_id = c.comp_id

)

select * from final