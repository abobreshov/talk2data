-- product_catalog.sql
-- Individual view definition for Databricks

USE CATALOG workspace;  -- Update with your catalog
USE SCHEMA default;    -- Update with your schema

-- =====================================================

CREATE OR REPLACE VIEW product_catalog AS
SELECT p.productId, p.`name`, p.brandName, p.sellingSize, p.category, p.subcategory, p.price_gbp, ps.sku AS primary_sku, count(DISTINCT ps2.sku) AS total_skus
FROM products AS p
LEFT JOIN product_skus AS ps ON (((p.productId = ps.productId) AND (ps.is_primary = TRUE)))
LEFT JOIN product_skus AS ps2 ON ((p.productId = ps2.productId))
GROUP BY p.productId, p.`name`, p.brandName, p.sellingSize, p.category, p.subcategory, p.price_gbp, ps.sku;;


-- =====================================================