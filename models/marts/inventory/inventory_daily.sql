
{{
    config(
        materialized='incremental',
        unique_key=['report_date', 'office_id', 'product_id'],
        sort=['comp_id', 'report_date'],
        dist='report_date'
    )
}}


with timezone as (

    select * from {{ ref('stg_io__timezone') }}

),

customers as (

    select * from {{ ref('int_customers') }}

),

product_transactions_raw as (

    select * from {{ ref('stg_io__product_transactions') }}

    {% if is_incremental() %}
        WHERE date > (SELECT max(report_date) - INTERVAL '2 DAY' FROM {{ this }})
    {% endif %}

),

product_transactions_tz as (

    SELECT 
        product_transactions_raw.*, 
        customers.domain_prefix,
        trunc(CONVERT_TIMEZONE('UTC', timezone.zone_name, date)) as report_date

    FROM product_transactions_raw

    INNER JOIN customers
        ON product_transactions_raw.comp_id = customers.comp_id

    INNER JOIN timezone
        ON customers.timezone_id = timezone.id

),

product_transactions as (

    SELECT *

    FROM product_transactions_tz

    {% if is_incremental() %}
        WHERE report_date > (SELECT max(report_date) FROM {{ this }}) AND report_date < current_date
    {% endif %}

),

units_of_weight as (

    select * from {{ ref('units_of_weight') }}

),

transactions_grouped AS (

    SELECT 
        product_transactions.report_date,
        product_transactions.comp_id,
        product_transactions.domain_prefix,
        product_transactions.office_id,
        product_id,
        COALESCE(units_of_weight.grams, 1) AS item_type_weight,

        COALESCE(SUM(CASE WHEN type IN (1) THEN qty
            WHEN type IN (7, 8) THEN -qty 
            ELSE NULL END) * item_type_weight, 0) AS check_in,
        0 AS transfer_in,
        COALESCE(SUM(CASE WHEN type IN (2, 10, 11) THEN -qty 
            ELSE NULL END
            ) * item_type_weight, 0) AS transfer_out,
        COALESCE(SUM(CASE WHEN type IN (21) THEN qty 
            ELSE NULL END
            ) * item_type_weight, 0) AS transfer_in_another_product,
        COALESCE(SUM(CASE WHEN type IN (22) THEN -qty 
            ELSE NULL END
            ) * item_type_weight, 0) AS transfer_out_another_product,
        COALESCE(SUM(CASE WHEN type IN (12, 15) THEN qty 
            ELSE NULL END
            ) * item_type_weight, 0) AS adjusted_increase,
        COALESCE(SUM(CASE WHEN type IN (13, 16) THEN -qty 
            ELSE NULL END
            ) * item_type_weight, 0) AS adjusted_decrease,
        COALESCE(SUM(CASE WHEN type IN (3) THEN -qty-qty_free 
            ELSE NULL END
            ) * item_type_weight, 0) AS sell,
        COALESCE(SUM(CASE WHEN type IN (9) THEN qty 
            ELSE NULL END
            ) * item_type_weight, 0) AS return

    FROM product_transactions
    
    LEFT JOIN units_of_weight
        ON product_transactions.item_type = units_of_weight.unit
    
    GROUP BY 1, 2, 3, 4, 5, 6

),

missed_transfers AS (

    SELECT 
        product_transactions.report_date,
        product_transactions.comp_id,
        product_transactions.domain_prefix,
        office_to_id AS office_id, 
        product_id,
        COALESCE(units_of_weight.grams, 1) AS item_type_weight,

        0 AS check_in,
        COALESCE(SUM(qty) * item_type_weight, 0) AS transfer_in,
        0 AS transfer_out,
        0 AS transfer_in_another_product,
        0 AS transfer_out_another_product,
        0 AS adjusted_increase,
        0 AS adjusted_decrease,
        0 AS sell,
        0 AS return

    FROM product_transactions

    LEFT JOIN units_of_weight
        ON product_transactions.item_type = units_of_weight.unit

    WHERE type = 2

    GROUP BY 1, 2, 3, 4, 5, 6

),

union_all AS (

    SELECT *
    FROM transactions_grouped

    UNION ALL

    SELECT *
    FROM missed_transfers

),

daily_total AS (

    SELECT 
        report_date, 
        comp_id,
        domain_prefix,
        CASE WHEN office_id = 0 OR office_id IS NULL
            THEN -comp_id ELSE office_id END as office_id,
        product_id,

        SUM(check_in) AS check_in,
        SUM(transfer_in) AS transfer_in,
        SUM(transfer_out) AS transfer_out,
        SUM(transfer_in_another_product) AS transfer_in_another_product,
        SUM(transfer_out_another_product) AS transfer_out_another_product,        
        SUM(adjusted_increase) AS adjusted_increase,
        SUM(adjusted_decrease) AS adjusted_decrease,
        SUM(sell) AS sell,
        SUM(return) AS return,
        SUM(check_in 
            + transfer_in 
            + transfer_out 
            + adjusted_increase 
            + adjusted_decrease 
            + sell 
            + return 
            + transfer_in_another_product 
            + transfer_out_another_product
        ) AS inventory_turnover  

    FROM union_all

    GROUP BY 1, 2, 3, 4, 5

),

end_of_day_calc as (

    SELECT 
        report_date, 
        comp_id,
        domain_prefix,
        office_id,
        product_id,
        check_in,
        transfer_in,
        transfer_out,
        adjusted_increase,
        adjusted_decrease,
        sell,
        return, 
        transfer_in_another_product,
        transfer_out_another_product,
        inventory_turnover
            
    FROM daily_total

),

offices as (
    select * from {{ ref('stg_io__offices') }}
),

int_products_with_details as (
    select * from {{ ref('int_products_with_details') }}
),

final as (

    select

        end_of_day_calc.report_date, 
        end_of_day_calc.comp_id,
        end_of_day_calc.domain_prefix,
        end_of_day_calc.office_id,
        CASE end_of_day_calc.office_id WHEN -1 THEN 'No office' ELSE offices.office_name END as office_name,
        end_of_day_calc.product_id,
        pwd.prod_name,
        pwd.brand_id,
        pwd.brand_name,
        pwd.direct_category,
        pwd.parent_category,
        pwd.sub_category_1,
        pwd.sub_category_2,
        end_of_day_calc.check_in,
        end_of_day_calc.transfer_in,
        end_of_day_calc.transfer_out,
        end_of_day_calc.adjusted_increase,
        end_of_day_calc.adjusted_decrease,
        end_of_day_calc.sell,
        end_of_day_calc.return, 
        end_of_day_calc.transfer_in_another_product,
        end_of_day_calc.transfer_out_another_product,
        end_of_day_calc.inventory_turnover

    from end_of_day_calc

    left join offices
        on end_of_day_calc.office_id = offices.office_id

    left join int_products_with_details pwd
        on end_of_day_calc.product_id = pwd.prod_id
        and end_of_day_calc.comp_id = pwd.comp_id

)

SELECT * FROM final

