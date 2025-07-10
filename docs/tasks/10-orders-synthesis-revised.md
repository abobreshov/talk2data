# Orders Synthesis Implementation Plan - REVISED

## Overview
Generate synthetic orders data by combining M5 Walmart dataset patterns with our products database, creating realistic order history for the past 2 years, along with a corresponding sales table.

## Updated Database Schema

### Orders Table
```sql
CREATE TABLE orders (
    orderId STRING PRIMARY KEY,
    customerId STRING,
    orderStatus STRING, -- CANCELLED, DELIVERED, PICKED, FUTURE
    orderDate TIMESTAMP,
    deliveryDate TIMESTAMP,
    totalPrice DECIMAL(10,2) -- in GBP
);
```

### Order Items Table
```sql
CREATE TABLE order_items (
    orderId STRING,
    productId STRING,
    quantity INTEGER,
    price DECIMAL(10,2), -- in GBP per unit
    FOREIGN KEY (orderId) REFERENCES orders(orderId),
    FOREIGN KEY (productId) REFERENCES products(productId)
);
```

### Sales Table (NEW)
```sql
CREATE TABLE sales (
    saleId STRING PRIMARY KEY,
    orderId STRING,
    productId STRING,
    saleDate TIMESTAMP,  -- equals deliveryDate from orders
    quantity INTEGER,
    price DECIMAL(10,2), -- in GBP per unit
    FOREIGN KEY (orderId) REFERENCES orders(orderId),
    FOREIGN KEY (productId) REFERENCES products(productId)
);
```

## Key Relationships

1. **Orders → Order Items**: One-to-many (one order has multiple items)
2. **Orders → Sales**: Order items become sales on delivery date
3. **Sales Date Logic**: 
   - Sales occur on `deliveryDate` (not `orderDate`)
   - Only DELIVERED orders generate sales records
   - CANCELLED orders do not generate sales

## Data Flow

```
M5 Data Analysis
    ↓
Product Mapping (M5 items → Our products)
    ↓
Customer Distribution (1-2 orders/week)
    ↓
Order Generation
    ├→ Orders table (with growing basket size £38→£43)
    ├→ Order_items table
    └→ Sales table (on delivery date)
```

## Implementation Changes

### Task 13: Product Mapping
- Map 1,437 M5 FOOD items to our 566 products
- Use M5 popularity data for weighted mapping
- Popular M5 items → Popular products (budget/standard tiers)

### Task 14: Customer Distribution
- No changes needed
- Still 1-2 orders per week per customer

### Task 15: Order Generation
- Generate orders with items
- Growing basket size (£38 → £43 over 2 years)
- Use M5 popularity for realistic item selection

### Task 16: Sales Table Generation (NEW)
- For each DELIVERED order:
  - Create sales records from order_items
  - Set saleDate = deliveryDate
  - Copy quantity and price from order_items
- No sales for CANCELLED orders
- Sales align perfectly with delivered orders

### Task 17: Validation (Updated)
- Validate orders as before
- Additionally validate:
  - Sales count matches delivered order items
  - Sales dates match delivery dates
  - No sales for cancelled orders

## Why This Approach?

1. **M5 Individual Sales**: M5 shows individual item sales, not baskets
2. **Our Orders**: We create realistic baskets (10-40 items)
3. **Sales Table**: Derived from our orders for consistency
4. **Business Logic**: Sales happen on delivery, not order placement

## Benefits

- Perfect alignment between orders and sales
- Realistic basket compositions
- Accurate sales reporting by date
- Supports future analytics (sales trends, forecasting)

## Next Steps

1. Update Task 13 script for product mapping
2. Keep Task 14 as is
3. Update Task 15 to prepare for sales generation
4. Create new Task 16 for sales table generation
5. Update validation to include sales checks