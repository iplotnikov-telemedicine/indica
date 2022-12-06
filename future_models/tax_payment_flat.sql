delete from looker_tmp.tax_payment_flat;
insert into looker_tmp.tax_payment_flat
SELECT
          order_id,
          "State Sales Tax" as tax_type,
          sum(state_sales_tax) as tax_amount
        FROM c9928_company.tax_payment
        WHERE state_sales_tax > 0
        GROUP BY 1,2

        UNION ALL

        SELECT
          order_id,
          "City Sales Tax" as tax_type,
          sum(city_sales_tax) as tax_amount
        FROM c9928_company.tax_payment
        WHERE city_sales_tax > 0
        GROUP BY 1,2

        UNION ALL

        SELECT
          order_id,
          "City Local Tax" as tax_type,
          sum(city_local_tax) as tax_amount
        FROM c9928_company.tax_payment
        WHERE city_local_tax > 0
        GROUP BY 1,2;