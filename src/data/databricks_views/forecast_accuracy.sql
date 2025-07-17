-- forecast_accuracy.sql
-- Individual view definition for Databricks

USE CATALOG workspace;  -- Update with your catalog
USE SCHEMA default;    -- Update with your schema

-- =====================================================

CREATE OR REPLACE VIEW forecast_accuracy AS
SELECT f.productId, f.forecast_date, f.target_date, f.forecast_horizon, f.predicted_quantity, s.actual_quantity, abs((f.predicted_quantity - s.actual_quantity)) AS absolute_error, CASE WHEN ((s.actual_quantity > 0)) THEN (((f.predicted_quantity - s.actual_quantity) / s.actual_quantity)) ELSE NULL END AS relative_error
FROM forecasts AS f
LEFT JOIN (SELECT productId, CAST(saleDate AS DATE) AS sale_date, sum(quantity) AS actual_quantity
FROM sales
GROUP BY productId, CAST(saleDate AS DATE)) AS s ON (((f.productId = s.productId) AND (f.target_date = s.sale_date)))
WHERE (s.actual_quantity IS NOT NULL);;


-- =====================================================