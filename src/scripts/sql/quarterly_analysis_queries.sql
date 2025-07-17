-- Quarterly Analysis Queries for Grocery POC Database
-- ===================================================

-- Query 1: Average Basket Size by Quarter
-- This query calculates the average basket size (order value) by quarter
WITH order_totals AS (
    SELECT 
        o.orderId,
        o.orderDate,
        EXTRACT(YEAR FROM o.orderDate) as order_year,
        EXTRACT(QUARTER FROM o.orderDate) as order_quarter,
        SUM(oi.quantity * oi.unitPrice) as order_total
    FROM orders o
    JOIN order_items oi ON o.orderId = oi.orderId
    WHERE o.orderStatus IN ('DELIVERED', 'PICKED')  -- Only completed orders
    AND o.orderDate >= CURRENT_DATE - INTERVAL '18 months'  -- Last 1.5 years
    GROUP BY o.orderId, o.orderDate
)
SELECT 
    order_year,
    order_quarter,
    order_year || '-Q' || order_quarter as quarter_label,
    COUNT(DISTINCT orderId) as total_orders,
    ROUND(AVG(order_total), 2) as avg_basket_size_gbp,
    ROUND(MIN(order_total), 2) as min_basket_gbp,
    ROUND(MAX(order_total), 2) as max_basket_gbp,
    ROUND(STDDEV(order_total), 2) as stddev_basket_gbp
FROM order_totals
GROUP BY order_year, order_quarter
ORDER BY order_year DESC, order_quarter DESC;

-- Query 2: Quarterly Sales for Last 1.5 Years
-- This query shows total sales revenue and volume by quarter
WITH quarterly_sales AS (
    SELECT 
        EXTRACT(YEAR FROM s.saleDate) as sale_year,
        EXTRACT(QUARTER FROM s.saleDate) as sale_quarter,
        s.productId,
        p.category,
        SUM(s.quantity) as units_sold,
        SUM(s.quantity * s.unitPrice * 100) as revenue_pence
    FROM sales s
    JOIN products p ON s.productId = p.productId
    WHERE s.saleDate >= CURRENT_DATE - INTERVAL '18 months'  -- Last 1.5 years
    GROUP BY 
        EXTRACT(YEAR FROM s.saleDate),
        EXTRACT(QUARTER FROM s.saleDate),
        s.productId,
        p.category
)
SELECT 
    sale_year,
    sale_quarter,
    sale_year || '-Q' || sale_quarter as quarter_label,
    COUNT(DISTINCT productId) as products_sold,
    SUM(units_sold) as total_units_sold,
    ROUND(SUM(revenue_pence) / 100.0, 2) as total_revenue_gbp,
    ROUND(AVG(revenue_pence) / 100.0, 2) as avg_sale_value_gbp,
    -- Category breakdown
    ROUND(SUM(CASE WHEN category = 'Fresh Food' THEN revenue_pence ELSE 0 END) / 100.0, 2) as fresh_food_revenue_gbp,
    ROUND(SUM(CASE WHEN category = 'Chilled Food' THEN revenue_pence ELSE 0 END) / 100.0, 2) as chilled_food_revenue_gbp,
    ROUND(SUM(CASE WHEN category = 'Food Cupboard' THEN revenue_pence ELSE 0 END) / 100.0, 2) as food_cupboard_revenue_gbp,
    ROUND(SUM(CASE WHEN category = 'Bakery' THEN revenue_pence ELSE 0 END) / 100.0, 2) as bakery_revenue_gbp
FROM quarterly_sales
GROUP BY sale_year, sale_quarter
ORDER BY sale_year DESC, sale_quarter DESC;

-- Query 3: Combined Quarterly Analysis - Basket Size and Sales Trends
-- This query combines basket metrics with sales performance
WITH order_metrics AS (
    SELECT 
        EXTRACT(YEAR FROM o.orderDate) as year,
        EXTRACT(QUARTER FROM o.orderDate) as quarter,
        COUNT(DISTINCT o.orderId) as order_count,
        COUNT(DISTINCT o.customerId) as active_customers,
        SUM(oi.quantity * oi.unitPrice) as total_order_value,
        SUM(oi.quantity) as total_items
    FROM orders o
    JOIN order_items oi ON o.orderId = oi.orderId
    WHERE o.orderStatus IN ('DELIVERED', 'PICKED')
    AND o.orderDate >= CURRENT_DATE - INTERVAL '18 months'
    GROUP BY 
        EXTRACT(YEAR FROM o.orderDate),
        EXTRACT(QUARTER FROM o.orderDate)
),
sales_metrics AS (
    SELECT 
        EXTRACT(YEAR FROM saleDate) as year,
        EXTRACT(QUARTER FROM saleDate) as quarter,
        SUM(quantity) as units_sold,
        SUM(quantity * unitPrice) as revenue_gbp
    FROM sales
    WHERE saleDate >= CURRENT_DATE - INTERVAL '18 months'
    GROUP BY 
        EXTRACT(YEAR FROM saleDate),
        EXTRACT(QUARTER FROM saleDate)
)
SELECT 
    om.year,
    om.quarter,
    om.year || '-Q' || om.quarter as quarter_label,
    om.order_count,
    om.active_customers,
    ROUND(om.total_order_value / om.order_count, 2) as avg_basket_size_gbp,
    ROUND(om.total_items::FLOAT / om.order_count, 1) as avg_items_per_basket,
    ROUND(om.order_count::FLOAT / om.active_customers, 1) as orders_per_customer,
    ROUND(sm.revenue_gbp, 2) as total_revenue_gbp,
    ROUND(sm.revenue_gbp / om.order_count, 2) as revenue_per_order_gbp,
    -- Quarter-over-quarter growth (compared to previous quarter)
    LAG(om.order_count) OVER (ORDER BY om.year, om.quarter) as prev_quarter_orders,
    ROUND(
        ((om.total_order_value / om.order_count) - 
         LAG(om.total_order_value / om.order_count) OVER (ORDER BY om.year, om.quarter)) / 
        LAG(om.total_order_value / om.order_count) OVER (ORDER BY om.year, om.quarter) * 100, 
        2
    ) as basket_size_qoq_growth_pct
FROM order_metrics om
LEFT JOIN sales_metrics sm ON om.year = sm.year AND om.quarter = sm.quarter
ORDER BY om.year DESC, om.quarter DESC;

-- Query 4: Year-over-Year Quarterly Comparison
-- Compare same quarters across different years
WITH quarterly_comparison AS (
    SELECT 
        EXTRACT(YEAR FROM o.orderDate) as year,
        EXTRACT(QUARTER FROM o.orderDate) as quarter,
        COUNT(DISTINCT o.orderId) as orders,
        SUM(oi.quantity * oi.unitPrice) as total_value,
        AVG(oi.quantity * oi.unitPrice) as avg_basket
    FROM orders o
    JOIN order_items oi ON o.orderId = oi.orderId
    WHERE o.orderStatus IN ('DELIVERED', 'PICKED')
    AND o.orderDate >= CURRENT_DATE - INTERVAL '18 months'
    GROUP BY 
        EXTRACT(YEAR FROM o.orderDate),
        EXTRACT(QUARTER FROM o.orderDate)
)
SELECT 
    quarter,
    MAX(CASE WHEN year = 2024 THEN orders END) as orders_2024,
    MAX(CASE WHEN year = 2025 THEN orders END) as orders_2025,
    MAX(CASE WHEN year = 2024 THEN ROUND(avg_basket, 2) END) as avg_basket_2024_gbp,
    MAX(CASE WHEN year = 2025 THEN ROUND(avg_basket, 2) END) as avg_basket_2025_gbp,
    CASE 
        WHEN MAX(CASE WHEN year = 2024 THEN avg_basket END) IS NOT NULL 
        AND MAX(CASE WHEN year = 2025 THEN avg_basket END) IS NOT NULL
        THEN ROUND(
            ((MAX(CASE WHEN year = 2025 THEN avg_basket END) - 
              MAX(CASE WHEN year = 2024 THEN avg_basket END)) / 
             MAX(CASE WHEN year = 2024 THEN avg_basket END)) * 100, 
            2
        )
        ELSE NULL
    END as yoy_basket_growth_pct
FROM quarterly_comparison
GROUP BY quarter
ORDER BY quarter;