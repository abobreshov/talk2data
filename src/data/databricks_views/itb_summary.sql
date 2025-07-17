-- itb_summary.sql
-- Individual view definition for Databricks

USE CATALOG workspace;  -- Update with your catalog
USE SCHEMA default;    -- Update with your schema

-- =====================================================

CREATE OR REPLACE VIEW itb_summary AS
SELECT lb.productId, p.`name` AS product_name, count(DISTINCT lb.session_id) AS active_sessions, sum(lb.quantity) AS total_quantity, avg(lb.quantity) AS avg_quantity_per_session, min(lb.added_at) AS earliest_add, max(lb.added_at) AS latest_add
FROM live_basket AS lb
INNER JOIN products AS p ON ((lb.productId = p.productId))
WHERE ((lb.status = 'ACTIVE') AND ((lb.expires_at IS NULL) OR (lb.expires_at > CURRENT_TIMESTAMP())))
GROUP BY lb.productId, p.`name`;;


-- =====================================================