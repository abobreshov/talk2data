-- future_purges.sql
-- Individual view definition for Databricks

USE CATALOG workspace;  -- Update with your catalog
USE SCHEMA default;    -- Update with your schema

-- =====================================================

CREATE OR REPLACE VIEW future_purges AS
SELECT s.sku, s.productId, p.`name` AS product_name, s.quantity_in_stock AS quantity_to_purge, s.expiration_date AS purge_date, s.batch_number, CASE WHEN ((s.expiration_date < CURRENT_DATE())) THEN ('EXPIRED') WHEN ((s.expiration_date = CURRENT_DATE())) THEN ('EXPIRING_TODAY') WHEN ((s.expiration_date <= (CURRENT_DATE() + INTERVAL 3 DAYS))) THEN ('EXPIRING_SOON') ELSE 'FUTURE' END AS purge_status
FROM stock AS s
INNER JOIN products AS p ON ((s.productId = p.productId))
WHERE ((s.quantity_in_stock > 0) AND (s.stock_status = 'AVAILABLE'))
ORDER BY s.expiration_date, s.productId;;


-- =====================================================