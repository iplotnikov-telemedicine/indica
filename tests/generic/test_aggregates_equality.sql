{% test aggregates_equality(model, column_name) %}

{% set compare_model_name = kwargs.get('compare_model_name') %}
{% set compare_model_column = kwargs.get('compare_model_column') %}
{% set compare_model_where = kwargs.get('compare_model_where') %}
{% set current_model_where = kwargs.get('current_model_where') %}
{% set diff_abs_threshold = kwargs.get('diff_abs_threshold') %}
{% set aggregate_type = kwargs.get('aggregate_type') %}
{% set is_distinct = kwargs.get('is_distinct') %}



with model as (
    select
       nvl( {{ aggregate_type }} ({{is_distinct or ''}} ({{ column_name }})) ,0) as agg_m
    from {{ model }}
    {{ current_model_where }}
),

compare as (
    select
        nvl( {{ aggregate_type }} ({{is_distinct or ''}} ({{ compare_model_column }})) ,0) as agg_c
    from {{ ref( compare_model_name ) }}
    {{ compare_model_where }}
),

differences as (
    select agg_m, agg_c, abs(agg_m - agg_c) as diff_abs
    from model
    join compare on 1=1
)

select * from differences where diff_abs > {{ diff_abs_threshold }}

{% endtest %}
