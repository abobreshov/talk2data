# Task 10: Orders Synthesis Overview

## Objective
Synthesize realistic orders data by combining M5 Walmart dataset with our products database, creating a comprehensive orders table with proper customer mapping and business rules.

## High-Level Requirements
1. Load M5 Walmart orders data
2. Map SKUs to FOOD category products from our products database
3. Generate orders for past 2 years until now
4. Map customers from customers.csv (1-2 orders per week per customer)
5. Apply business rules for order status, pricing, and timing

## Database Schema
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
    price DECIMAL(10,2), -- in GBP
    FOREIGN KEY (orderId) REFERENCES orders(orderId),
    FOREIGN KEY (productId) REFERENCES products(productId)
);
```

## Business Rules
1. **Order Status Logic:**
   - deliveryDate < today: DELIVERED (99.5%) or CANCELLED (0.5%)
   - deliveryDate = today: PICKED
   - deliveryDate > today: FUTURE

2. **Pricing Rules:**
   - Total order value: £20-£100
   - Average order value: ~£38
   - Product prices in database are in pennies (divide by 100)

3. **Customer Distribution:**
   - 1-2 orders per week per customer
   - Even distribution across time period

4. **Delivery Timing:**
   - deliveryDate = orderDate + random(1-7 days)

## Implementation Tasks
- Task 11: Environment Setup and Data Validation
- Task 12: M5 Dataset Loading and Analysis
- Task 13: Product Mapping Strategy
- Task 14: Customer Distribution Algorithm
- Task 15: Order Generation Logic
- Task 16: Order Items Generation
- Task 17: Data Validation and Quality Checks
- Task 18: Performance Optimization
- Task 19: Final Integration and Testing

## Success Criteria
- 2 years of synthetic order data
- Realistic customer ordering patterns
- Proper status distribution
- Order values within specified ranges
- All foreign key constraints satisfied