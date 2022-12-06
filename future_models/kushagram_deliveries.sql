DELETE FROM looker_tmp.kushagram_deliveries;
INSERT INTO looker_tmp.kushagram_deliveries
SELECT
    orders.id  AS `order_id`,
    MIN(CASE WHEN  warehouse_order_logs.type   in (19, 39) THEN warehouse_order_logs.created_at END)  AS `delivery_start_time`,
    MIN(CASE WHEN  warehouse_order_logs.type   in (2) THEN warehouse_order_logs.created_at END)  AS `accepted_time`,
    MAX(CASE WHEN  warehouse_order_logs.type   in (7, 8) THEN warehouse_order_logs.created_at END)  AS `delivery_end_time`
FROM c3628_company.warehouse_order_logs  AS warehouse_order_logs
LEFT JOIN c3628_company.warehouse_orders AS orders ON warehouse_order_logs.order_id= orders.id
WHERE (orders.type ) = 'delivery'
GROUP BY  1;