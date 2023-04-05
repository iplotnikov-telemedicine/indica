{# 'The model only use is to expand historical periods for the "inventory_poq" model' #}
{# 'Incremental type is used to prevent meaningless recalculation of inventory history' #}
{# 'For full refresh use the command "dbt run -s inventory_history --full-refresh"' #}


{{
  config(
    materialized='incremental',
    unique_key=['date', 'office_id', 'product_id'],
    sort=['comp_id', 'date'],
    dist='date'
  )
}}


{# 'Define a set of inventory transactions on the necessary history range' #}
with turnovers as (
    select * from {{ ref('transactions_daily') }}
    where report_date <= '2023-02-13'::date
      and report_date >= '2022-12-01'::date 
),

{# 'Define the inventory level for the end of the last day of the history range' #}
poq_130223 as (
    select *
    from {{ ref('inventory_poq') }}
    where date = '2023-02-13'::date 
)

{# 'Define the days list query for the history range' #}
{% set hist_dates_query %}
    select date_day::date
    from {{ ref('util_dates') }}
    where date_day >= '2022-12-01'::date
      and date_day < '2023-02-13'::date
    order by date_day::date desc
{% endset %}


{# 'Execute query into loop iteration tuple. Make it trivial if incremental run' #}
{% set results = run_query(hist_dates_query) %}
{% if execute and flags.FULL_REFRESH %}
{% set hist_dates = results.columns[0].values() %}
{% else %}
{% set hist_dates = ['3000-01-01'] %}
{% endif %}


{# 'Loop through the history days by casting "i" date to date from the "agate" row' #}
{% for i in hist_dates %}

  {# 'Calculate and sum end-of-day inventory for each history date as follow' #}
  select
    comp_id,
    '{{i}}'::date as date,
    product_id,
    office_id,
    sum(inventory_poq) as inventory_poq
    
  from
      (
      {# 'Inventory on 2023-02-13 minus the sum of transactions since the "i" date' #}
      select * from poq_130223
      union all
      
      select
        comp_id,
        null as report_date,
        product_id,
        office_id,
        sum(-inventory_turnover) as inventory_turnover
      from turnovers
      where report_date >= '{{i}}'::date + 1
      group by 1, 3, 4
      ) 

  {# 'Unsatisfiable conditions for incremental update' #}
  where '{{i}}' != '3000-01-01'
  group by 1, 3, 4
  --having sum(inventory_poq) > 0

  {# 'Append results for the "i" via "union all"' #}
  {% if not loop.last %}
    UNION ALL
  {% endif %}

{% endfor %}
