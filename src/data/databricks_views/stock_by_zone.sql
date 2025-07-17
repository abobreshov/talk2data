-- stock_by_zone.sql
-- Individual view definition for Databricks

USE CATALOG workspace;  -- Update with your catalog
USE SCHEMA default;    -- Update with your schema

-- =====================================================

CREATE OR REPLACE VIEW stock_by_zone AS
SELECT temperature_zone, count(DISTINCT productId) AS num_products, count(DISTINCT sku) AS num_skus, sum(quantity_in_stock) AS total_items, count(DISTINCT batch_number) AS num_batches
FROM stock
WHERE (stock_status = 'AVAILABLE')
GROUP BY temperature_zone
ORDER BY temperature_zone;;


-- =====================================================