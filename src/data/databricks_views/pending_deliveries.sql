-- pending_deliveries.sql
-- Individual view definition for Databricks

USE CATALOG workspace;  -- Update with your catalog
USE SCHEMA default;    -- Update with your schema

-- =====================================================

CREATE OR REPLACE VIEW pending_deliveries AS
SELECT id.productId, p.`name` AS product_name, id.expected_delivery_date, sum(id.quantity) AS total_quantity, count(DISTINCT id.po_id) AS num_orders, avg(id.unit_cost) AS avg_unit_cost
FROM inbound_deliveries AS id
INNER JOIN products AS p ON ((id.productId = p.productId))
WHERE ((id.status = 'PENDING') AND (id.expected_delivery_date >= CURRENT_DATE()))
GROUP BY id.productId, p.`name`, id.expected_delivery_date
ORDER BY id.expected_delivery_date, id.productId;;


-- =====================================================