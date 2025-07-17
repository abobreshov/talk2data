# Inventory Management Guide

## Table Dependencies Diagram

```
products (master table)
    ├── product_skus (1:many - SKU mapping)
    ├── stock (current inventory by SKU)
    ├── forecasts (demand predictions)
    ├── live_basket (unfulfilled demand)
    ├── inbound_deliveries (pending stock)
    ├── product_ordering_calendar (ordering rules)
    ├── sales (historical sales)
    ├── order_items (order details)
    └── purge_log (expired stock tracking)

suppliers
    ├── products (supplier_id FK)
    ├── supplier_schedules (delivery patterns)
    └── purchase_orders (orders to suppliers)

customers
    └── orders (customer orders)
        └── order_items (order details)
            └── sales (fulfilled items)

purchase_orders
    └── inbound_deliveries (expected stock)
```

## Key Table Relationships

### Core Inventory Tables

1. **products** - Master product catalog
   - Primary Key: `productId` (18-digit string)
   - Links to: All product-related tables

2. **stock** - Current inventory levels
   - Foreign Key: `productId` → products
   - Key Fields: `sku`, `quantity_in_stock`, `expiration_date`, `stock_status`

3. **forecasts** - Demand predictions
   - Foreign Key: `productId` → products
   - Key Fields: `target_date`, `predicted_quantity`

4. **inbound_deliveries** - Pending stock arrivals
   - Foreign Keys: `productId` → products, `po_id` → purchase_orders
   - Key Fields: `expected_delivery_date`, `quantity`, `status`

5. **live_basket** - Unfulfilled demand (ITB - In The Basket)
   - Foreign Key: `productId` → products
   - Key Fields: `target_date`, `quantity`, `status`

## SQL Queries for Inventory Management

### 1. When will I run out of stock for a product?

```sql
-- Calculate stockout date for a specific product
WITH stock_projection AS (
    SELECT 
        p.productId,
        p.name as product_name,
        -- Current available stock
        COALESCE(s.total_stock, 0) as current_stock,
        -- Average daily demand from forecasts
        COALESCE(f.avg_daily_demand, 0) as daily_demand,
        -- Days until stockout
        CASE 
            WHEN COALESCE(f.avg_daily_demand, 0) = 0 THEN 999
            ELSE COALESCE(s.total_stock, 0) / COALESCE(f.avg_daily_demand, 0)
        END as days_until_stockout,
        -- Projected stockout date
        CASE 
            WHEN COALESCE(f.avg_daily_demand, 0) = 0 THEN NULL
            ELSE CURRENT_DATE + CAST(COALESCE(s.total_stock, 0) / COALESCE(f.avg_daily_demand, 0) AS INTEGER)
        END as estimated_stockout_date
    FROM products p
    LEFT JOIN (
        SELECT 
            productId,
            SUM(quantity_in_stock) as total_stock
        FROM stock
        WHERE stock_status = 'AVAILABLE'
        GROUP BY productId
    ) s ON p.productId = s.productId
    LEFT JOIN (
        SELECT 
            productId,
            AVG(predicted_quantity) as avg_daily_demand
        FROM forecasts
        WHERE target_date >= CURRENT_DATE
        AND target_date <= CURRENT_DATE + INTERVAL '7 days'
        GROUP BY productId
    ) f ON p.productId = f.productId
)
SELECT 
    productId,
    product_name,
    current_stock,
    ROUND(daily_demand, 2) as avg_daily_demand,
    ROUND(days_until_stockout, 1) as days_until_stockout,
    estimated_stockout_date,
    CASE 
        WHEN days_until_stockout <= 1 THEN 'CRITICAL'
        WHEN days_until_stockout <= 3 THEN 'HIGH_RISK'
        WHEN days_until_stockout <= 7 THEN 'MEDIUM_RISK'
        ELSE 'LOW_RISK'
    END as risk_level
FROM stock_projection
WHERE productId = ? -- Replace with specific productId
ORDER BY days_until_stockout;
```

### 2. What is the nearest purge date for a product?

```sql
-- Find the nearest expiration/purge date for a product
SELECT 
    p.productId,
    p.name as product_name,
    s.sku,
    s.quantity_in_stock,
    s.expiration_date,
    s.expiration_date - CURRENT_DATE as days_until_expiration,
    CASE 
        WHEN s.expiration_date <= CURRENT_DATE THEN 'EXPIRED'
        WHEN s.expiration_date <= CURRENT_DATE + INTERVAL '3 days' THEN 'EXPIRING_SOON'
        WHEN s.expiration_date <= CURRENT_DATE + INTERVAL '7 days' THEN 'WARNING'
        ELSE 'OK'
    END as expiration_status
FROM products p
JOIN stock s ON p.productId = s.productId
WHERE p.productId = ? -- Replace with specific productId
AND s.stock_status = 'AVAILABLE'
AND s.quantity_in_stock > 0
ORDER BY s.expiration_date
LIMIT 1;
```

### 3. Do I have enough stock for the next N days?

```sql
-- Check if stock covers demand for next N days
WITH demand_forecast AS (
    SELECT 
        productId,
        SUM(predicted_quantity) as total_demand_next_n_days
    FROM forecasts
    WHERE target_date >= CURRENT_DATE
    AND target_date < CURRENT_DATE + INTERVAL '? days' -- Replace ? with number of days (1-7)
    GROUP BY productId
),
current_inventory AS (
    SELECT 
        productId,
        SUM(quantity_in_stock) as current_stock
    FROM stock
    WHERE stock_status = 'AVAILABLE'
    GROUP BY productId
),
pending_deliveries AS (
    SELECT 
        productId,
        SUM(quantity) as incoming_stock
    FROM inbound_deliveries
    WHERE status = 'PENDING'
    AND expected_delivery_date >= CURRENT_DATE
    AND expected_delivery_date < CURRENT_DATE + INTERVAL '? days' -- Same number of days
    GROUP BY productId
)
SELECT 
    p.productId,
    p.name as product_name,
    COALESCE(ci.current_stock, 0) as current_stock,
    COALESCE(pd.incoming_stock, 0) as incoming_stock,
    COALESCE(ci.current_stock, 0) + COALESCE(pd.incoming_stock, 0) as total_available,
    COALESCE(df.total_demand_next_n_days, 0) as expected_demand,
    CASE 
        WHEN COALESCE(ci.current_stock, 0) + COALESCE(pd.incoming_stock, 0) >= COALESCE(df.total_demand_next_n_days, 0) 
        THEN 'YES'
        ELSE 'NO'
    END as sufficient_stock,
    COALESCE(ci.current_stock, 0) + COALESCE(pd.incoming_stock, 0) - COALESCE(df.total_demand_next_n_days, 0) as surplus_or_shortage
FROM products p
LEFT JOIN demand_forecast df ON p.productId = df.productId
LEFT JOIN current_inventory ci ON p.productId = ci.productId
LEFT JOIN pending_deliveries pd ON p.productId = pd.productId
WHERE p.productId = ?; -- Replace with specific productId
```

### 4. When is the nearest delivery for a product?

```sql
-- Find next scheduled delivery for a product
SELECT 
    p.productId,
    p.name as product_name,
    id.delivery_id,
    id.expected_delivery_date,
    id.expected_delivery_date - CURRENT_DATE as days_until_delivery,
    id.quantity as incoming_quantity,
    id.sku,
    po.po_id,
    po.supplier_id,
    s.supplier_name
FROM products p
JOIN inbound_deliveries id ON p.productId = id.productId
JOIN purchase_orders po ON id.po_id = po.po_id
JOIN suppliers s ON po.supplier_id = s.supplier_id
WHERE p.productId = ? -- Replace with specific productId
AND id.status = 'PENDING'
AND id.expected_delivery_date >= CURRENT_DATE
ORDER BY id.expected_delivery_date
LIMIT 1;
```

### 5. Products at risk of stockout in next 1-3 days

```sql
-- Find all products at risk of stockout within specified days
WITH risk_analysis AS (
    SELECT 
        p.productId,
        p.name as product_name,
        p.category,
        -- Current stock
        COALESCE(s.current_stock, 0) as current_stock,
        -- Demand for next N days
        COALESCE(f.demand_next_n_days, 0) as expected_demand,
        -- Incoming deliveries in next N days
        COALESCE(i.incoming_stock, 0) as incoming_stock,
        -- Net position
        COALESCE(s.current_stock, 0) + COALESCE(i.incoming_stock, 0) - COALESCE(f.demand_next_n_days, 0) as net_position
    FROM products p
    LEFT JOIN (
        SELECT 
            productId,
            SUM(quantity_in_stock) as current_stock
        FROM stock
        WHERE stock_status = 'AVAILABLE'
        GROUP BY productId
    ) s ON p.productId = s.productId
    LEFT JOIN (
        SELECT 
            productId,
            SUM(predicted_quantity) as demand_next_n_days
        FROM forecasts
        WHERE target_date >= CURRENT_DATE
        AND target_date < CURRENT_DATE + INTERVAL '? days' -- Replace ? with 1, 2, or 3
        GROUP BY productId
    ) f ON p.productId = f.productId
    LEFT JOIN (
        SELECT 
            productId,
            SUM(quantity) as incoming_stock
        FROM inbound_deliveries
        WHERE status = 'PENDING'
        AND expected_delivery_date >= CURRENT_DATE
        AND expected_delivery_date < CURRENT_DATE + INTERVAL '? days' -- Same as above
        GROUP BY productId
    ) i ON p.productId = i.productId
)
SELECT 
    productId,
    product_name,
    category,
    current_stock,
    expected_demand,
    incoming_stock,
    net_position,
    CASE 
        WHEN net_position < 0 THEN 'STOCKOUT'
        WHEN net_position = 0 THEN 'ZERO_STOCK'
        WHEN net_position < expected_demand * 0.2 THEN 'CRITICAL_LOW'
        ELSE 'LOW_STOCK'
    END as risk_status
FROM risk_analysis
WHERE net_position < expected_demand * 0.5 -- Less than 50% of demand covered
ORDER BY net_position ASC, expected_demand DESC
LIMIT 50;
```

## Usage Instructions

### For AI Agents

1. **Parameter Replacement**: Replace all `?` placeholders with actual values:
   - `productId`: Use the 18-digit product identifier (e.g., '000000000100053315')
   - Days: Use integers 1-7 for day ranges

2. **Query Selection**:
   - Use Query 1 for "when will we run out" questions
   - Use Query 2 for expiration/purge date questions
   - Use Query 3 for stock sufficiency checks
   - Use Query 4 for delivery schedule questions
   - Use Query 5 for risk assessment across multiple products

3. **Result Interpretation**:
   - `days_until_stockout` < 1: Immediate action required
   - `risk_level` = 'CRITICAL': Order immediately
   - `sufficient_stock` = 'NO': Need to expedite orders
   - `net_position` < 0: Will definitely run out

### For Humans

1. **Connect to Database**: Use DuckDB or export to your preferred database system

2. **Replace Parameters**:
   ```sql
   -- Example: Check stock for product '000000000100053315' for next 3 days
   WHERE p.productId = '000000000100053315'
   AND target_date < CURRENT_DATE + INTERVAL '3 days'
   ```

3. **Combine Queries**: For comprehensive analysis, run multiple queries:
   - First check current stock status (Query 3)
   - Then check delivery schedule (Query 4)
   - Finally assess stockout risk (Query 1)

4. **Action Triggers**:
   - **Immediate Order**: When `days_until_stockout` ≤ lead time
   - **Expedite Delivery**: When `risk_level` = 'CRITICAL'
   - **Plan Promotions**: When `surplus_or_shortage` > 0 and expiration approaching

## Example Analysis Flow

```sql
-- Step 1: Identify at-risk products for tomorrow
WITH tomorrow_risk AS (
    -- Use Query 5 with 1 day parameter
)

-- Step 2: For each at-risk product, check deliveries
WITH delivery_check AS (
    -- Use Query 4 for each at-risk product
)

-- Step 3: Calculate emergency order requirements
WITH emergency_orders AS (
    SELECT 
        productId,
        product_name,
        ABS(net_position) as units_needed,
        supplier_id,
        lead_time_days
    FROM risk_analysis r
    JOIN product_ordering_calendar poc ON r.productId = poc.productId
    WHERE net_position < 0
)

-- Step 4: Generate action report
SELECT * FROM emergency_orders
ORDER BY units_needed DESC;
```

## Performance Tips

1. **Create Indexes**:
   ```sql
   CREATE INDEX idx_stock_product ON stock(productId, stock_status);
   CREATE INDEX idx_forecast_date ON forecasts(target_date, productId);
   CREATE INDEX idx_inbound_date ON inbound_deliveries(expected_delivery_date, status);
   ```

2. **Use Materialized Views** for frequently accessed calculations

3. **Schedule Regular Updates** of the stock_projection view

## Integration Notes

- All queries return standard result sets compatible with any SQL client
- Results can be exported to JSON/CSV for API consumption
- Consider caching results for products with stable demand patterns
- Implement alerts based on risk_level thresholds