{{ dbt_utils.date_spine(
    "day",
    "to_date('01/01/2015', 'mm/dd/yyyy')",
    "dateadd(month, 1, current_date)"
)
}}