-- stock_summary.sql
-- Individual view definition for Databricks

USE CATALOG workspace;  -- Update with your catalog
USE SCHEMA default;    -- Update with your schema

-- =====================================================

CREATE OR REPLACE VIEW stock_summary AS
SELECT s.productId, p.`name` AS product_name, p.category, p.subcategory, count(DISTINCT s.sku) AS num_skus, count(DISTINCT s.batch_number) AS num_batches, sum(s.quantity_in_stock) AS total_stock, min(s.expiration_date) AS earliest_expiration, max(s.expiration_date) AS latest_expiration, sum(CASE WHEN ((s.expiration_date <= (CURRENT_DATE() + INTERVAL 7 DAYS))) THEN (s.quantity_in_stock) ELSE 0 END) AS expiring_soon_qty
FROM stock AS s
INNER JOIN products AS p ON ((s.productId = p.productId))
WHERE (s.stock_status = 'AVAILABLE')
GROUP BY s.productId, p.`name`, p.category, p.subcategory;;


-- =====================================================