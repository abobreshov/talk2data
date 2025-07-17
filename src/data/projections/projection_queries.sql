-- Stock Projection Usage Examples

-- 1. View daily projection for a specific product
SELECT * FROM stock_projection 
WHERE productId = '000000000100053315'
ORDER BY projection_date;

-- 2. Get all products at risk of stockout tomorrow
SELECT 
    productId,
    product_name,
    category,
    beginning_stock,
    inbound_deliveries,
    total_demand,
    projected_ending_stock
FROM stock_projection
WHERE projection_date = CURRENT_DATE + INTERVAL '1 day'
AND stock_status = 'STOCKOUT_RISK'
ORDER BY total_demand DESC;

-- 3. Daily summary by temperature zone
SELECT * FROM stock_projection_daily_summary
ORDER BY projection_date, temp_zone;

-- 4. Products needing urgent reorder
SELECT * FROM stock_projection_alerts
WHERE stockout_days >= 3
ORDER BY max_daily_demand DESC;

-- 5. Calculate total storage needs by date
SELECT 
    projection_date,
    temp_zone,
    SUM(projected_ending_stock + inbound_deliveries) as total_storage_needed
FROM stock_projection
GROUP BY projection_date, temp_zone
ORDER BY projection_date, temp_zone;

-- 6. Products with excessive stock (>30 days supply)
SELECT 
    productId,
    product_name,
    category,
    beginning_stock,
    AVG(forecast_demand) as avg_daily_demand,
    MIN(days_of_supply) as days_of_supply
FROM stock_projection
WHERE days_of_supply > 30
GROUP BY productId, product_name, category, beginning_stock
ORDER BY days_of_supply DESC;
