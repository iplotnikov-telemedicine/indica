
{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key=['report_date', 'office_id', 'product_id'],
        sort=['comp_id', 'report_date'],
        dist='report_date'
    )
}}


with product_transactions as (

    select * from {{ ref('stg_io__product_transactions') }}
    {% if is_incremental() %}
        where date >= (select max(report_date) - interval '1 DAY' from {{ this }})
    {% endif %}

),

companies as (

    select * from {{ ref('stg_io__companies') }}

),

timezone as (

    select * from {{ ref('stg_io__timezone') }}

),

units_of_weight as (

    select * from {{ ref('units_of_weight') }}

),

transactions_grouped AS (

    SELECT 
        trunc(CONVERT_TIMEZONE('UTC', timezone.zone_name, date)) as report_date,
        product_transactions.comp_id,
        companies.domain_prefix,
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

    INNER JOIN companies
        ON product_transactions.comp_id = companies.comp_id
    
    INNER JOIN timezone
        ON companies.timezone_id = timezone.id
    
    LEFT JOIN units_of_weight
        ON product_transactions.item_type = units_of_weight.unit
    
    GROUP BY 1, 2, 3, 4, 5, 6

),

missed_transfers AS (

    SELECT 
        trunc(CONVERT_TIMEZONE('UTC', timezone.zone_name, date)) as report_date,
        product_transactions.comp_id,
        companies.domain_prefix,
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

    INNER JOIN companies
        ON product_transactions.comp_id = companies.comp_id
    
    INNER JOIN timezone
        ON companies.timezone_id = timezone.id

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
        office_id, 
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
        inventory_turnover,
        SUM(inventory_turnover) OVER (PARTITION BY comp_id, office_id, product_id 
            ORDER BY report_date ROWS UNBOUNDED PRECEDING) AS end_of_day_inventory
            
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
        coalesce(end_of_day_calc.office_id, 0) as office_id,
        CASE coalesce(end_of_day_calc.office_id, 0) WHEN 0 THEN 'No office' ELSE offices.office_name END as office_name,
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

    inner join int_products_with_details pwd
        on end_of_day_calc.product_id = pwd.prod_id
        and end_of_day_calc.comp_id = pwd.comp_id

)

SELECT * FROM final

