-- latest_forecasts.sql
-- Individual view definition for Databricks

USE CATALOG workspace;  -- Update with your catalog
USE SCHEMA default;    -- Update with your schema

-- =====================================================

CREATE OR REPLACE VIEW latest_forecasts AS

SELECT f.*
FROM forecasts f
INNER JOIN (
    SELECT productId, MAX(forecast_date) as max_forecast_date
    FROM forecasts
    GROUP BY productId
) latest ON f.productId = latest.productId AND f.forecast_date = latest.max_forecast_date;


-- =====================================================