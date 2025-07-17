-- Stock Analysis Queries for Grocery POC Database
-- =================================================

-- Query 1: Check if we have enough stock for 2 days
-- This query calculates average daily sales and compares with current stock
WITH daily_sales_avg AS (
    -- Calculate average daily sales for each product over the last 7 days
    SELECT 
        productId,
        AVG(daily_quantity) as avg_daily_sales,
        COUNT(DISTINCT sale_date) as days_with_sales
    FROM (
        SELECT 
            productId,
            DATE_TRUNC('day', saleDate) as sale_date,
            SUM(quantity) as daily_quantity
        FROM sales
        WHERE saleDate >= CURRENT_DATE - INTERVAL '7 days'
        GROUP BY productId, DATE_TRUNC('day', saleDate)
    ) daily_sales
    GROUP BY productId
),
current_stock AS (
    -- Get current stock levels by product
    SELECT 
        productId,
        SUM(quantity_in_stock) as total_stock,
        MIN(expiration_date) as earliest_expiration
    FROM stock
    WHERE stock_status = 'AVAILABLE'
    GROUP BY productId
)
SELECT 
    p.productId,
    p.name as product_name,
    p.category,
    p.subcategory,
    COALESCE(cs.total_stock, 0) as current_stock,
    ROUND(COALESCE(dsa.avg_daily_sales, 0), 2) as avg_daily_sales,
    ROUND(COALESCE(dsa.avg_daily_sales, 0) * 2, 2) as needed_for_2_days,
    CASE 
        WHEN COALESCE(cs.total_stock, 0) >= COALESCE(dsa.avg_daily_sales, 0) * 2 THEN 'SUFFICIENT'
        WHEN COALESCE(cs.total_stock, 0) > 0 THEN 'LOW'
        ELSE 'OUT_OF_STOCK'
    END as stock_status,
    CASE 
        WHEN COALESCE(dsa.avg_daily_sales, 0) > 0 
        THEN ROUND(COALESCE(cs.total_stock, 0) / dsa.avg_daily_sales, 1)
        ELSE NULL 
    END as days_of_stock_remaining,
    cs.earliest_expiration
FROM products p
LEFT JOIN daily_sales_avg dsa ON p.productId = dsa.productId
LEFT JOIN current_stock cs ON p.productId = cs.productId
WHERE dsa.avg_daily_sales > 0  -- Only show products with recent sales
ORDER BY 
    CASE 
        WHEN COALESCE(cs.total_stock, 0) >= COALESCE(dsa.avg_daily_sales, 0) * 2 THEN 3
        WHEN COALESCE(cs.total_stock, 0) > 0 THEN 2
        ELSE 1
    END,
    dsa.avg_daily_sales DESC;

-- Query 2: Products that may run out of stock in 2 days
-- This specifically focuses on products at risk
WITH daily_demand AS (
    SELECT 
        productId,
        AVG(daily_quantity) as avg_daily_demand,
        MAX(daily_quantity) as max_daily_demand,
        STDDEV(daily_quantity) as demand_stddev
    FROM (
        SELECT 
            productId,
            DATE_TRUNC('day', saleDate) as sale_date,
            SUM(quantity) as daily_quantity
        FROM sales
        WHERE saleDate >= CURRENT_DATE - INTERVAL '14 days'  -- 2 weeks for more stable average
        GROUP BY productId, DATE_TRUNC('day', saleDate)
    ) daily_sales
    GROUP BY productId
),
stock_levels AS (
    SELECT 
        productId,
        SUM(quantity_in_stock) as available_stock,
        MIN(expiration_date) as next_expiration,
        SUM(CASE WHEN expiration_date <= CURRENT_DATE + INTERVAL '2 days' THEN quantity_in_stock ELSE 0 END) as expiring_in_2_days
    FROM stock
    WHERE stock_status = 'AVAILABLE'
    GROUP BY productId
),
pending_orders AS (
    -- Check for incoming purchase orders
    SELECT 
        ps.productId,
        SUM(po.quantity_ordered) as incoming_quantity,
        MIN(po.expected_delivery_date) as next_delivery
    FROM purchase_orders po
    JOIN product_skus ps ON po.sku = ps.sku
    WHERE po.status IN ('PENDING', 'SHIPPED')
    AND po.expected_delivery_date <= CURRENT_DATE + INTERVAL '2 days'
    GROUP BY ps.productId
)
SELECT 
    p.productId,
    p.name as product_name,
    p.category,
    p.subcategory,
    COALESCE(sl.available_stock, 0) as current_stock,
    ROUND(dd.avg_daily_demand, 2) as avg_daily_demand,
    ROUND(dd.max_daily_demand, 2) as peak_daily_demand,
    ROUND(dd.avg_daily_demand * 2, 2) as expected_2day_demand,
    ROUND(dd.max_daily_demand * 2, 2) as peak_2day_demand,
    COALESCE(sl.available_stock, 0) - ROUND(dd.avg_daily_demand * 2, 2) as stock_surplus_deficit,
    COALESCE(po.incoming_quantity, 0) as incoming_stock,
    po.next_delivery,
    sl.expiring_in_2_days,
    CASE 
        WHEN COALESCE(sl.available_stock, 0) < dd.avg_daily_demand * 2 THEN 'HIGH_RISK'
        WHEN COALESCE(sl.available_stock, 0) < dd.max_daily_demand * 2 THEN 'MEDIUM_RISK'
        ELSE 'LOW_RISK'
    END as risk_level,
    CASE 
        WHEN dd.avg_daily_demand > 0 
        THEN ROUND(COALESCE(sl.available_stock, 0) / dd.avg_daily_demand, 1)
        ELSE 999 
    END as days_of_coverage
FROM products p
INNER JOIN daily_demand dd ON p.productId = dd.productId
LEFT JOIN stock_levels sl ON p.productId = sl.productId
LEFT JOIN pending_orders po ON p.productId = po.productId
WHERE dd.avg_daily_demand > 0
AND (
    COALESCE(sl.available_stock, 0) < dd.max_daily_demand * 2  -- At risk based on peak demand
    OR sl.expiring_in_2_days > 0  -- Has expiring stock
)
ORDER BY 
    CASE 
        WHEN COALESCE(sl.available_stock, 0) < dd.avg_daily_demand * 2 THEN 1
        WHEN COALESCE(sl.available_stock, 0) < dd.max_daily_demand * 2 THEN 2
        ELSE 3
    END,
    stock_surplus_deficit ASC;

-- Query 3: Summary dashboard - Overall stock health
WITH stock_analysis AS (
    SELECT 
        p.productId,
        p.category,
        COALESCE(SUM(s.quantity_in_stock), 0) as current_stock,
        COALESCE(AVG(daily_sales.avg_daily), 0) as avg_daily_demand,
        CASE 
            WHEN COALESCE(AVG(daily_sales.avg_daily), 0) > 0 
            THEN COALESCE(SUM(s.quantity_in_stock), 0) / AVG(daily_sales.avg_daily)
            ELSE 999
        END as days_coverage
    FROM products p
    LEFT JOIN stock s ON p.productId = s.productId AND s.stock_status = 'AVAILABLE'
    LEFT JOIN (
        SELECT 
            productId,
            AVG(daily_quantity) as avg_daily
        FROM (
            SELECT 
                productId,
                DATE_TRUNC('day', saleDate) as sale_date,
                SUM(quantity) as daily_quantity
            FROM sales
            WHERE saleDate >= CURRENT_DATE - INTERVAL '7 days'
            GROUP BY productId, DATE_TRUNC('day', saleDate)
        ) daily
        GROUP BY productId
    ) daily_sales ON p.productId = daily_sales.productId
    GROUP BY p.productId, p.category
)
SELECT 
    'Overall' as category,
    COUNT(DISTINCT productId) as total_products,
    COUNT(DISTINCT CASE WHEN days_coverage < 2 THEN productId END) as products_low_stock,
    COUNT(DISTINCT CASE WHEN days_coverage = 0 THEN productId END) as products_out_of_stock,
    ROUND(100.0 * COUNT(DISTINCT CASE WHEN days_coverage < 2 THEN productId END) / COUNT(DISTINCT productId), 2) as pct_at_risk
FROM stock_analysis
WHERE avg_daily_demand > 0

UNION ALL

SELECT 
    category,
    COUNT(DISTINCT productId) as total_products,
    COUNT(DISTINCT CASE WHEN days_coverage < 2 THEN productId END) as products_low_stock,
    COUNT(DISTINCT CASE WHEN days_coverage = 0 THEN productId END) as products_out_of_stock,
    ROUND(100.0 * COUNT(DISTINCT CASE WHEN days_coverage < 2 THEN productId END) / COUNT(DISTINCT productId), 2) as pct_at_risk
FROM stock_analysis
WHERE avg_daily_demand > 0
GROUP BY category
ORDER BY 
    CASE WHEN category = 'Overall' THEN 0 ELSE 1 END,
    pct_at_risk DESC;