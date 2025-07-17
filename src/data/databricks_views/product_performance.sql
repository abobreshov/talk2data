-- product_performance.sql
-- Individual view definition for Databricks

USE CATALOG workspace;  -- Update with your catalog
USE SCHEMA default;    -- Update with your schema

-- =====================================================

CREATE OR REPLACE VIEW product_performance AS
SELECT p.productId, p.`name`, p.brandName, p.category, p.subcategory, p.price_gbp, count(DISTINCT s.orderId) AS times_ordered, sum(s.quantity) AS units_sold, sum((s.unitPrice * s.quantity)) AS revenue, rank() OVER (ORDER BY sum(s.quantity) DESC) AS sales_rank
FROM products AS p
LEFT JOIN sales AS s ON ((p.productId = s.productId))
GROUP BY p.productId, p.`name`, p.brandName, p.category, p.subcategory, p.price_gbp;;


-- =====================================================