-- customer_analytics.sql
-- Individual view definition for Databricks

USE CATALOG workspace;  -- Update with your catalog
USE SCHEMA default;    -- Update with your schema

-- =====================================================

CREATE OR REPLACE VIEW customer_analytics AS
SELECT c.customerId, c.first_name, c.last_name, c.email, c.city, c.postcode, count(DISTINCT o.orderId) AS total_orders, sum(CASE WHEN ((o.orderStatus = 'DELIVERED')) THEN (1) ELSE 0 END) AS delivered_orders, sum(CASE WHEN ((o.orderStatus = 'CANCELLED')) THEN (1) ELSE 0 END) AS cancelled_orders, sum(CASE WHEN ((o.orderStatus = 'DELIVERED')) THEN (o.totalAmount) ELSE 0 END) AS lifetime_value, avg(CASE WHEN ((o.orderStatus = 'DELIVERED')) THEN (o.totalAmount) ELSE NULL END) AS avg_order_value, min(o.orderDate) AS first_order, max(o.orderDate) AS last_order
FROM customers AS c
LEFT JOIN orders AS o ON ((c.customerId = o.customerId))
GROUP BY c.customerId, c.first_name, c.last_name, c.email, c.city, c.postcode;;


-- =====================================================