-- supplier_delivery_calendar.sql
-- Individual view definition for Databricks

USE CATALOG workspace;  -- Update with your catalog
USE SCHEMA default;    -- Update with your schema

-- =====================================================

CREATE OR REPLACE VIEW supplier_delivery_calendar AS
SELECT s.supplier_id, s.supplier_name, s.contact_email, ss.delivery_date, ss.po_cutoff_date, ss.po_cutoff_time, ss.lead_time_days, CASE 
        WHEN DAYOFWEEK(ss.delivery_date) = 2 THEN 'Monday'
        WHEN DAYOFWEEK(ss.delivery_date) = 3 THEN 'Tuesday'
        WHEN DAYOFWEEK(ss.delivery_date) = 4 THEN 'Wednesday'
        WHEN DAYOFWEEK(ss.delivery_date) = 5 THEN 'Thursday'
        WHEN DAYOFWEEK(ss.delivery_date) = 6 THEN 'Friday'
        ELSE NULL 
    END AS delivery_day_of_week
FROM supplier_schedules AS ss
INNER JOIN suppliers AS s ON ((ss.supplier_id = s.supplier_id));;


-- =====================================================
-- Verification Queries
-- =====================================================
-- Run these after creating all views to verify they work:

-- SELECT COUNT(*) FROM customer_analytics;  -- Expected: 1000
-- SELECT COUNT(*) FROM forecast_accuracy;  -- Expected: 0
-- SELECT COUNT(*) FROM future_purges;  -- Expected: 4007
-- SELECT COUNT(*) FROM itb_summary;  -- Expected: 3
-- SELECT COUNT(*) FROM latest_forecasts;  -- Expected: 16569
-- SELECT COUNT(*) FROM pending_deliveries;  -- Expected: 453
-- SELECT COUNT(*) FROM product_catalog;  -- Expected: 2501
-- SELECT COUNT(*) FROM product_expiration_guide;  -- Expected: 2501
-- SELECT COUNT(*) FROM product_performance;  -- Expected: 2501
-- SELECT COUNT(*) FROM stock_availability;  -- Expected: 2479
-- SELECT COUNT(*) FROM stock_by_zone;  -- Expected: 2
-- SELECT COUNT(*) FROM stock_summary;  -- Expected: 2479
-- SELECT COUNT(*) FROM supplier_delivery_calendar;  -- Expected: 30613
