-- Databricks View Definitions
-- Generated from: /home/abobreshov/work/dataart/talk2data/grocery_poc/src/data/grocery_final.db
-- Generated on: 2025-07-16 18:15:52
-- Total views: 13

-- INSTRUCTIONS:
-- 1. Upload all table data to Databricks first (use Parquet files)
-- 2. Create a database/schema in Databricks for your tables
-- 3. Run this script in Databricks SQL or a notebook
-- 4. Replace 'your_schema.' with your actual schema name if needed

USE CATALOG your_catalog;  -- Replace with your catalog name
USE SCHEMA your_schema;    -- Replace with your schema name



-- View: customer_analytics
------------------------------------------------------------
CREATE OR REPLACE VIEW customer_analytics AS
SELECT c.customerId, c.first_name, c.last_name, c.email, c.city, c.postcode, count(DISTINCT o.orderId) AS total_orders, sum(CASE WHEN ((o.orderStatus = 'DELIVERED')) THEN (1) ELSE 0 END) AS delivered_orders, sum(CASE WHEN ((o.orderStatus = 'CANCELLED')) THEN (1) ELSE 0 END) AS cancelled_orders, sum(CASE WHEN ((o.orderStatus = 'DELIVERED')) THEN (o.totalAmount) ELSE 0 END) AS lifetime_value, avg(CASE WHEN ((o.orderStatus = 'DELIVERED')) THEN (o.totalAmount) ELSE NULL END) AS avg_order_value, min(o.orderDate) AS first_order, max(o.orderDate) AS last_order FROM customers AS c LEFT JOIN orders AS o ON ((c.customerId = o.customerId)) GROUP BY c.customerId, c.first_name, c.last_name, c.email, c.city, c.postcode;;


-- View: forecast_accuracy
------------------------------------------------------------
CREATE OR REPLACE VIEW forecast_accuracy AS
SELECT f.productId, f.forecast_date, f.target_date, f.forecast_horizon, f.predicted_quantity, s.actual_quantity, abs((f.predicted_quantity - s.actual_quantity)) AS absolute_error, CASE WHEN ((s.actual_quantity > 0)) THEN (((f.predicted_quantity - s.actual_quantity) / s.actual_quantity)) ELSE NULL END AS relative_error FROM forecasts AS f LEFT JOIN (SELECT productId, CAST(saleDate AS DATE) AS sale_date, sum(quantity) AS actual_quantity FROM sales GROUP BY productId, CAST(saleDate AS DATE)) AS s ON (((f.productId = s.productId) AND (f.target_date = s.sale_date))) WHERE (s.actual_quantity IS NOT NULL);;


-- View: future_purges
------------------------------------------------------------
CREATE OR REPLACE VIEW future_purges AS
SELECT s.sku, s.productId, p.name AS product_name, s.quantity_in_stock AS quantity_to_purge, s.expiration_date AS purge_date, s.batch_number, CASE WHEN ((s.expiration_date < CURRENT_DATE())) THEN ('EXPIRED') WHEN ((s.expiration_date = CURRENT_DATE())) THEN ('EXPIRING_TODAY') WHEN ((s.expiration_date <= (CURRENT_DATE() + CAST('3 days' AS INTERVAL)))) THEN ('EXPIRING_SOON') ELSE 'FUTURE' END AS purge_status FROM stock AS s INNER JOIN products AS p ON ((s.productId = p.productId)) WHERE ((s.quantity_in_stock > 0) AND (s.stock_status = 'AVAILABLE')) ORDER BY s.expiration_date, s.productId;;


-- View: itb_summary
------------------------------------------------------------
CREATE OR REPLACE VIEW itb_summary AS
SELECT lb.productId, p.name AS product_name, count(DISTINCT lb.session_id) AS active_sessions, sum(lb.quantity) AS total_quantity, avg(lb.quantity) AS avg_quantity_per_session, min(lb.added_at) AS earliest_add, max(lb.added_at) AS latest_add FROM live_basket AS lb INNER JOIN products AS p ON ((lb.productId = p.productId)) WHERE ((lb.status = 'ACTIVE') AND ((lb.expires_at IS NULL) OR (lb.expires_at > CURRENT_TIMESTAMP()))) GROUP BY lb.productId, p.name;;


-- View: latest_forecasts
------------------------------------------------------------
CREATE OR REPLACE VIEW latest_forecasts AS
SELECT * FROM forecasts WHERE ("row"(productId, forecast_date) = ANY(SELECT productId, max(forecast_date) FROM forecasts GROUP BY productId));;


-- View: pending_deliveries
------------------------------------------------------------
CREATE OR REPLACE VIEW pending_deliveries AS
SELECT id.productId, p.name AS product_name, id.expected_delivery_date, sum(id.quantity) AS total_quantity, count(DISTINCT id.po_id) AS num_orders, avg(id.unit_cost) AS avg_unit_cost FROM inbound_deliveries AS id INNER JOIN products AS p ON ((id.productId = p.productId)) WHERE ((id.status = 'PENDING') AND (id.expected_delivery_date >= CURRENT_DATE())) GROUP BY id.productId, p.name, id.expected_delivery_date ORDER BY id.expected_delivery_date, id.productId;;


-- View: product_catalog
------------------------------------------------------------
CREATE OR REPLACE VIEW product_catalog AS
SELECT p.productId, p.name, p.brandName, p.sellingSize, p.category, p.subcategory, p.price_gbp, ps.sku AS primary_sku, count(DISTINCT ps2.sku) AS total_skus FROM products AS p LEFT JOIN product_skus AS ps ON (((p.productId = ps.productId) AND (ps.is_primary = CAST('t' AS BOOLEAN)))) LEFT JOIN product_skus AS ps2 ON ((p.productId = ps2.productId)) GROUP BY p.productId, p.name, p.brandName, p.sellingSize, p.category, p.subcategory, p.price_gbp, ps.sku;;


-- View: product_expiration_guide
------------------------------------------------------------
CREATE OR REPLACE VIEW product_expiration_guide AS
SELECT p.productId, p.name AS product_name, p.category, p.subcategory, ppr.shelf_life_days, ppr.min_shelf_life_days, ppr.max_shelf_life_days, ppr.temperature_sensitive, ppr.requires_refrigeration, ppr.notes AS storage_notes, p.price_pence, p.price_gbp FROM products AS p LEFT JOIN product_purge_reference AS ppr ON (((p.category = ppr.category) AND ((p.subcategory = ppr.subcategory) OR ((p.subcategory IS NULL) AND (ppr.subcategory IS NULL))))) ORDER BY ppr.shelf_life_days, p.category, p.subcategory;;


-- View: product_performance
------------------------------------------------------------
CREATE OR REPLACE VIEW product_performance AS
SELECT p.productId, p.name, p.brandName, p.category, p.subcategory, p.price_gbp, count(DISTINCT s.orderId) AS times_ordered, sum(s.quantity) AS units_sold, sum((s.unitPrice * s.quantity)) AS revenue, rank() OVER (ORDER BY sum(s.quantity) DESC) AS sales_rank FROM products AS p LEFT JOIN sales AS s ON ((p.productId = s.productId)) GROUP BY p.productId, p.name, p.brandName, p.category, p.subcategory, p.price_gbp;;


-- View: stock_availability
------------------------------------------------------------
CREATE OR REPLACE VIEW stock_availability AS
SELECT s.productId, p.name AS product_name, p.category, count(DISTINCT s.sku) AS num_skus, sum(s.quantity_in_stock) AS total_quantity, min(s.expiration_date) AS earliest_expiration, sum(CASE WHEN ((s.expiration_date <= (CURRENT_DATE() + CAST('7 days' AS INTERVAL)))) THEN (s.quantity_in_stock) ELSE 0 END) AS expiring_week_qty, avg(s.purchase_price) AS avg_purchase_price FROM stock AS s INNER JOIN products AS p ON ((s.productId = p.productId)) WHERE (s.stock_status = 'AVAILABLE') GROUP BY s.productId, p.name, p.category;;


-- View: stock_by_zone
------------------------------------------------------------
CREATE OR REPLACE VIEW stock_by_zone AS
SELECT temperature_zone, count(DISTINCT productId) AS num_products, count(DISTINCT sku) AS num_skus, sum(quantity_in_stock) AS total_items, count(DISTINCT batch_number) AS num_batches FROM stock WHERE (stock_status = 'AVAILABLE') GROUP BY temperature_zone ORDER BY temperature_zone;;


-- View: stock_summary
------------------------------------------------------------
CREATE OR REPLACE VIEW stock_summary AS
SELECT s.productId, p.name AS product_name, p.category, p.subcategory, count(DISTINCT s.sku) AS num_skus, count(DISTINCT s.batch_number) AS num_batches, sum(s.quantity_in_stock) AS total_stock, min(s.expiration_date) AS earliest_expiration, max(s.expiration_date) AS latest_expiration, sum(CASE WHEN ((s.expiration_date <= (CURRENT_DATE() + CAST('7 days' AS INTERVAL)))) THEN (s.quantity_in_stock) ELSE 0 END) AS expiring_soon_qty FROM stock AS s INNER JOIN products AS p ON ((s.productId = p.productId)) WHERE (s.stock_status = 'AVAILABLE') GROUP BY s.productId, p.name, p.category, p.subcategory;;


-- View: supplier_delivery_calendar
------------------------------------------------------------
CREATE OR REPLACE VIEW supplier_delivery_calendar AS
SELECT s.supplier_id, s.supplier_name, s.contact_email, ss.delivery_date, ss.po_cutoff_date, ss.po_cutoff_time, ss.lead_time_days, CASE WHEN ((date_part('DOW', ss.delivery_date) = 1)) THEN ('Monday') WHEN ((date_part('DOW', ss.delivery_date) = 2)) THEN ('Tuesday') WHEN ((date_part('DOW', ss.delivery_date) = 3)) THEN ('Wednesday') WHEN ((date_part('DOW', ss.delivery_date) = 4)) THEN ('Thursday') WHEN ((date_part('DOW', ss.delivery_date) = 5)) THEN ('Friday') ELSE NULL END AS delivery_day_of_week FROM supplier_schedules AS ss INNER JOIN suppliers AS s ON ((ss.supplier_id = s.supplier_id));;


-- Summary of Views
-- ================
-- customer_analytics: 1,000 rows
-- forecast_accuracy: 0 rows
-- future_purges: 4,007 rows
-- itb_summary: 3 rows
-- latest_forecasts: 16,569 rows
-- pending_deliveries: 453 rows
-- product_catalog: 2,501 rows
-- product_expiration_guide: 2,501 rows
-- product_performance: 2,501 rows
-- stock_availability: 2,479 rows
-- stock_by_zone: 2 rows
-- stock_summary: 2,479 rows
-- supplier_delivery_calendar: 30,613 rows
