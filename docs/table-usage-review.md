# Table Usage Review: Base Tables and PO Generation Logic

## Overview

This document reviews how base tables in the grocery POC database are utilized in the Purchase Order (PO) generation logic. The system follows a demand-driven replenishment model where forecasts drive ordering decisions.

## Core Table Relationships in PO Generation

```
forecasts (demand signal)
    ↓
product_ordering_calendar (ordering rules)
    ↓
purchase_orders (supplier orders)
    ↓
inbound_deliveries (expected receipts)
    ↓
stock (available inventory)
    ↓
sales (fulfilled demand)
```

## Table Usage Analysis

### 1. Products Table
**Primary Role**: Master catalog maintaining product information

**Key Fields**:
- `productId` (PK): 18-digit unique identifier
- `supplier_id` (FK): Links to suppliers table
- `category`, `subcategory`: Determines temperature zone
- `price_pence`: Used for order value calculations

**PO Generation Usage**:
- Source of product attributes for ordering decisions
- Temperature zone determination for capacity planning
- Supplier assignment for order routing
- Price calculations for purchase order totals

```sql
-- Example: Getting products for ordering
SELECT 
    p.productId,
    p.name,
    p.category,
    p.supplier_id,
    CASE 
        WHEN p.category = 'Chilled Food' THEN 'CHILLED'
        WHEN p.category = 'Frozen Food' THEN 'FROZEN'
        ELSE 'AMBIENT'
    END as temp_zone
FROM products p
WHERE p.supplier_id IS NOT NULL;
```

### 2. Forecasts Table
**Primary Role**: Demand predictions that drive replenishment

**Key Fields**:
- `forecast_id` (PK): Unique forecast entry
- `productId` (FK): Links to products
- `target_date`: Date of predicted demand
- `predicted_quantity`: Expected sales units
- `forecast_date`: When forecast was generated

**PO Generation Usage**:
- Primary input for calculating order quantities
- Determines timing of orders (when demand expected)
- Multiple forecast entries per product/date (use latest)

```sql
-- Example: Get latest forecast for ordering
SELECT 
    f1.productId,
    f1.target_date,
    f1.predicted_quantity
FROM forecasts f1
WHERE f1.forecast_date = (
    SELECT MAX(f2.forecast_date)
    FROM forecasts f2
    WHERE f2.productId = f1.productId
    AND f2.target_date = f1.target_date
);
```

### 3. Stock Table
**Primary Role**: Current inventory position by SKU

**Key Fields**:
- `stock_id` (PK): Unique stock entry
- `sku`: Specific batch identifier
- `productId` (FK): Links to products
- `quantity_in_stock`: Available units
- `expiration_date`: For FIFO/FEFO logic
- `stock_status`: AVAILABLE, RESERVED, etc.

**PO Generation Usage**:
- Calculate net requirements (demand minus current stock)
- Only 'AVAILABLE' status counted for ordering
- Aggregated to product level for order decisions

```sql
-- Example: Current stock by product
SELECT 
    productId,
    SUM(quantity_in_stock) as total_stock,
    MIN(expiration_date) as nearest_expiry
FROM stock
WHERE stock_status = 'AVAILABLE'
GROUP BY productId;
```

### 4. Product Ordering Calendar
**Primary Role**: Defines ordering rules and constraints per product-supplier combination

**Key Fields**:
- `calendar_id` (PK): Unique entry
- `productId` (FK): Links to products
- `supplier_id` (FK): Links to suppliers
- `atp_delivery_date`: Available-to-promise delivery date
- `lead_time_days`: Days between order and delivery
- `min_order_quantity`: Minimum units per order
- `order_multiple`: Quantity rounding factor
- `contingency_days_of_demand`: Safety stock in days

**PO Generation Logic**:
```
Order Date = ATP Delivery Date - Lead Time Days
Order Quantity = MAX(
    min_order_quantity,
    ROUND_UP(
        (forecast_demand * contingency_days_of_demand) - current_stock,
        order_multiple
    )
)
```

### 5. Purchase Orders Table
**Primary Role**: Header records for orders placed with suppliers

**Key Fields**:
- `po_id` (PK): Unique order identifier
- `supplier_id` (FK): Links to suppliers
- `order_date`: When order was placed
- `expected_delivery_date`: When goods expected
- `po_status`: PENDING, RECEIVED, CANCELLED
- `total_items`: Sum of quantities ordered
- `total_amount`: Order value

**PO Generation Usage**:
- Parent record for grouped product orders
- One PO per supplier per delivery date
- Status tracking for order lifecycle

### 6. Inbound Deliveries Table
**Primary Role**: Line items for expected product receipts

**Key Fields**:
- `delivery_id` (PK): Unique delivery line
- `po_id` (FK): Links to purchase_orders
- `productId` (FK): Links to products
- `sku`: Generated as productId-YYYYMMDD-batch
- `quantity`: Units to be delivered
- `expected_delivery_date`: Arrival date
- `expiration_date`: Calculated shelf life
- `status`: PENDING, RECEIVED, CANCELLED

**PO Generation Usage**:
- Creates new SKUs for incoming inventory
- Links ordered quantities to specific products
- Tracks what's in transit for net requirement calculations

```sql
-- Example: Pending deliveries affect net requirements
SELECT 
    productId,
    SUM(quantity) as incoming_stock,
    MIN(expected_delivery_date) as next_delivery
FROM inbound_deliveries
WHERE status = 'PENDING'
AND expected_delivery_date >= CURRENT_DATE
GROUP BY productId;
```

### 7. Suppliers Table
**Primary Role**: Supplier master data

**Key Fields**:
- `supplier_id` (PK): Unique identifier
- `supplier_name`: Display name
- `contact_email`, `contact_phone`: Communication
- `address`: Delivery origin
- `is_active`: Supplier availability

**PO Generation Usage**:
- Groups products by supplier for ordering
- Contact information for order transmission
- Active flag filters available suppliers

### 8. Supplier Schedules Table
**Primary Role**: Defines when suppliers deliver and accept orders

**Key Fields**:
- `schedule_id` (PK): Unique entry
- `supplier_id` (FK): Links to suppliers
- `delivery_date`: Specific delivery date
- `po_cutoff_date`: Last date to place order
- `po_cutoff_time`: Daily order deadline
- `lead_time_days`: Notice required

**PO Generation Usage**:
- Determines valid delivery dates
- Enforces order cutoff times
- Validates lead time requirements

```sql
-- Example: Find next valid delivery date for supplier
SELECT 
    MIN(delivery_date) as next_delivery
FROM supplier_schedules
WHERE supplier_id = ?
AND delivery_date >= CURRENT_DATE + lead_time_days
AND po_cutoff_date >= CURRENT_DATE;
```

### 9. Live Basket Table
**Primary Role**: Tracks unfulfilled demand from stockouts

**Key Fields**:
- `basket_id` (PK): Unique entry
- `productId` (FK): Links to products
- `target_date`: When demand occurred
- `quantity`: Unfulfilled units
- `fulfillment_percentage`: Partial fulfillment tracking
- `status`: ACTIVE, FULFILLED, EXPIRED

**PO Generation Enhancement**:
- Adds to forecast demand for catch-up ordering
- Represents real unmet customer demand
- Priority fulfillment when stock arrives

```sql
-- Example: Total demand including ITB
SELECT 
    COALESCE(f.productId, lb.productId) as productId,
    COALESCE(f.predicted_quantity, 0) as forecast_demand,
    COALESCE(lb.quantity, 0) as itb_demand,
    COALESCE(f.predicted_quantity, 0) + COALESCE(lb.quantity, 0) as total_demand
FROM forecasts f
FULL OUTER JOIN live_basket lb 
    ON f.productId = lb.productId 
    AND f.target_date = lb.target_date
WHERE f.target_date = CURRENT_DATE + 3;
```

### 10. Sales Table
**Primary Role**: Historical record of completed transactions

**Key Fields**:
- `sale_id` (PK): Unique transaction
- `order_item_id` (FK): Links to order_items
- `productId` (FK): Links to products
- `quantity_sold`: Units sold
- `saleDate`: Transaction date

**PO Generation Usage**:
- Historical data for forecast validation
- Actual vs predicted analysis
- Seasonality pattern detection

### 11. Product SKUs Table
**Primary Role**: Maps products to multiple warehouse SKUs

**Key Fields**:
- `mapping_id` (PK): Unique mapping
- `productId` (FK): Links to products
- `sku`: Warehouse identifier
- `sku_name`: Description
- `is_primary`: Main SKU flag

**PO Generation Usage**:
- Reference for SKU generation patterns
- Warehouse system integration
- Multiple SKU handling per product

### 12. Orders & Order Items Tables
**Primary Role**: Customer order management

**Key Fields (orders)**:
- `orderId` (PK): Unique order
- `customerId` (FK): Links to customers
- `orderStatus`: PENDING, DELIVERED, CANCELLED

**Key Fields (order_items)**:
- `orderItemId` (PK): Unique line item
- `orderId` (FK): Links to orders
- `productId` (FK): Links to products
- `quantity`: Units ordered

**PO Generation Usage**:
- Source of actual demand patterns
- Stockout incidents create live_basket entries
- Order fulfillment consumes stock

### 13. Product Purge Reference Table
**Primary Role**: Defines auto-purge rules by product category

**Key Fields**:
- `purge_ref_id` (PK): Unique rule
- `category`, `subcategory`: Product classification
- `days_before_expiry`: When to purge
- `purge_reason`: Explanation

**PO Generation Usage**:
- Affects available shelf life calculations
- Influences order quantities (account for purge losses)
- Risk factor in safety stock calculations

### 14. Purge Log Table
**Primary Role**: Records inventory removed from stock

**Key Fields**:
- `purge_id` (PK): Unique purge event
- `sku`, `productId`: What was purged
- `quantity_purged`: Units removed
- `purge_date`: When removed
- `purge_reason`: Why removed

**PO Generation Usage**:
- Historical purge rates affect ordering
- Identify products with high waste
- Adjust order quantities to minimize purge

## PO Generation Process Flow

### Step 1: Identify What to Order
```sql
-- Products needing orders based on forecast and stock
SELECT 
    poc.productId,
    poc.supplier_id,
    poc.atp_delivery_date,
    f.predicted_quantity * poc.contingency_days_of_demand as gross_requirement,
    COALESCE(s.current_stock, 0) as current_stock,
    COALESCE(id.incoming_stock, 0) as in_transit
FROM product_ordering_calendar poc
JOIN (
    -- Latest forecasts
    SELECT productId, SUM(predicted_quantity) as predicted_quantity
    FROM forecasts
    WHERE target_date BETWEEN ? AND ?
    GROUP BY productId
) f ON poc.productId = f.productId
LEFT JOIN (
    -- Current stock
    SELECT productId, SUM(quantity_in_stock) as current_stock
    FROM stock
    WHERE stock_status = 'AVAILABLE'
    GROUP BY productId
) s ON poc.productId = s.productId
LEFT JOIN (
    -- In-transit stock
    SELECT productId, SUM(quantity) as incoming_stock
    FROM inbound_deliveries
    WHERE status = 'PENDING'
    GROUP BY productId
) id ON poc.productId = id.productId
WHERE poc.atp_delivery_date = CURRENT_DATE + 3;
```

### Step 2: Calculate Order Quantities
```sql
-- Apply ordering rules
SELECT 
    productId,
    supplier_id,
    CASE 
        WHEN net_requirement <= 0 THEN 0
        WHEN net_requirement < min_order_quantity THEN min_order_quantity
        ELSE CEILING(net_requirement::FLOAT / order_multiple) * order_multiple
    END as order_quantity
FROM (
    SELECT 
        productId,
        supplier_id,
        gross_requirement - current_stock - in_transit as net_requirement,
        min_order_quantity,
        order_multiple
    FROM ordering_opportunities
) calc
WHERE net_requirement > 0;
```

### Step 3: Check Capacity Constraints
```sql
-- Ensure daily capacity limits not exceeded
WITH daily_capacity AS (
    SELECT 
        expected_delivery_date,
        CASE 
            WHEN p.category = 'Chilled Food' THEN 'CHILLED'
            WHEN p.category = 'Frozen Food' THEN 'FROZEN'
            ELSE 'AMBIENT'
        END as temp_zone,
        SUM(od.quantity) as total_quantity
    FROM order_decisions od
    JOIN products p ON od.productId = p.productId
    GROUP BY expected_delivery_date, temp_zone
)
SELECT * FROM daily_capacity
WHERE (temp_zone = 'CHILLED' AND total_quantity > 15000)
   OR (temp_zone = 'FROZEN' AND total_quantity > 10000)
   OR (temp_zone = 'AMBIENT' AND total_quantity > 25000);
```

### Step 4: Create Purchase Orders and Deliveries
```sql
-- Insert PO header
INSERT INTO purchase_orders (
    po_id, supplier_id, order_date, 
    expected_delivery_date, po_status, 
    total_items, total_amount
)
SELECT 
    'PO_' || TO_CHAR(CURRENT_DATE, 'YYYYMMDD') || '_' || supplier_id,
    supplier_id,
    CURRENT_DATE,
    delivery_date,
    'PENDING',
    SUM(quantity),
    SUM(quantity * unit_cost)
FROM order_decisions
GROUP BY supplier_id, delivery_date;

-- Insert delivery lines
INSERT INTO inbound_deliveries (
    delivery_id, po_id, productId, sku,
    quantity, unit_cost, expected_delivery_date,
    expiration_date, status
)
SELECT 
    'DEL_' || nextval('delivery_seq'),
    po_id,
    productId,
    productId || '-' || TO_CHAR(delivery_date, 'YYYYMMDD') || '-001',
    quantity,
    unit_cost,
    delivery_date,
    delivery_date + shelf_life_days,
    'PENDING'
FROM order_line_items;
```

## Key Design Principles

1. **Product-Level Ordering**: Orders placed for products, not SKUs
2. **SKU Generation**: New SKUs created upon delivery (productId-date-batch)
3. **FIFO/FEFO**: Enforced through expiration_date in stock table
4. **Capacity Management**: Daily limits by temperature zone
5. **Safety Stock**: Controlled by contingency_days parameters
6. **Supplier Constraints**: Delivery schedules and lead times enforced

## Summary

The table structure supports sophisticated inventory management where:
- **Forecasts** drive demand planning
- **Stock** provides current position
- **Product Ordering Calendar** sets the rules
- **Purchase Orders** and **Inbound Deliveries** manage the flow
- **Live Basket** captures unmet demand
- All tables work together to maintain optimal inventory levels while minimizing waste and stockouts