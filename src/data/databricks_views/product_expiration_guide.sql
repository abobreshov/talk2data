-- product_expiration_guide.sql
-- Individual view definition for Databricks

USE CATALOG workspace;  -- Update with your catalog
USE SCHEMA default;    -- Update with your schema

-- =====================================================

CREATE OR REPLACE VIEW product_expiration_guide AS
SELECT p.productId, p.`name` AS product_name, p.category, p.subcategory, ppr.shelf_life_days, ppr.min_shelf_life_days, ppr.max_shelf_life_days, ppr.temperature_sensitive, ppr.requires_refrigeration, ppr.notes AS storage_notes, p.price_pence, p.price_gbp
FROM products AS p
LEFT JOIN product_purge_reference AS ppr ON (((p.category = ppr.category) AND ((p.subcategory = ppr.subcategory) OR ((p.subcategory IS NULL) AND (ppr.subcategory IS NULL)))))
ORDER BY ppr.shelf_life_days, p.category, p.subcategory;;


-- =====================================================