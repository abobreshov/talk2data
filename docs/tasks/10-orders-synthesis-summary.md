# Orders Synthesis Implementation Plan Summary

## Overview
Generate synthetic orders data by combining M5 Walmart dataset patterns with our products database, creating realistic order history for the past 2 years.

## Task Breakdown

### Task 10: Orders Synthesis Overview
- Defines project objectives and database schema
- Establishes business rules and success criteria

### Task 11: Environment Setup and Data Validation
- Set up Python environment (grocery_poc conda)
- Validate data sources:
  - Products database (`src/data/products.duckdb`)
  - Customers data (`customers.csv`)
  - M5 dataset (via `datasetsforecast.m5` package)
- Initialize orders database with proper schema

### Task 12: M5 Dataset Loading and Analysis
- Load M5 data using `datasetsforecast.m5.M5.load()`
- Understand data structure:
  - `train_df`: 47M+ sales records
  - `X_df`: Pricing and event data
  - `S_df`: Product metadata
- Extract FOODS category items (identified by `unique_id` starting with "FOODS")
- Analyze sales patterns and temporal distributions

### Task 13: Product Mapping Strategy
- Map ~1,437 M5 FOOD items to our product catalog
- Create weighted distribution:
  - Budget items (< £2): 40%
  - Standard items (£2-5): 35%
  - Premium items (£5-10): 20%
  - Luxury items (> £10): 5%
- Save mapping for consistent order generation

### Task 14: Customer Distribution Algorithm
- Design algorithm for 1-2 orders per week per customer
- Create customer shopping behaviors:
  - Shopping frequency (once vs twice weekly)
  - Preferred shopping days
  - Consistency patterns
- Generate order schedule for 2 years

### Task 15: Order Generation Logic
- Generate complete orders using M5 basket patterns
- Implement business rules:
  - **Growing basket size**: £38 → £43 over 2 years
  - Order range: £20-£100
  - 0.5% cancellation rate
- Create order items with realistic product combinations

### Task 16: Sales Table Generation
- Generate sales records from delivered orders
- Sales occur on delivery_date (not order_date)
- No sales for cancelled orders
- Each order item becomes one sale record
- Validate sales-orders alignment

### Task 17: Final Validation and Testing
- Comprehensive validation of all business rules
- Validate sales table integrity
- Generate validation report
- Create sample queries for analysis
- Ensure data quality and consistency

## Key Implementation Changes

### 1. Growing Basket Size
- **Start**: £38 average (2 years ago)
- **End**: £43 average (today)
- **Implementation**: Linear growth over time
  ```python
  days_elapsed = (order_date - start_date).days
  total_days = (end_date - start_date).days
  progress = days_elapsed / total_days
  target_average = 38 + (5 * progress)  # £38 to £43
  ```

### 2. Price Handling
- **Products database**: Prices in pennies (divide by 100 for GBP)
- **M5 prices**: In USD, convert to GBP for mapping (1 USD ≈ 0.8 GBP)
- **All calculations**: Use GBP after conversion

### 3. Order Generation Strategy
- Use M5 patterns for item selection
- Adjust quantities to meet target order value
- Add randomness: ±20% variation around target average
- Ensure all orders stay within £20-£100 range

## Expected Outputs

1. **Database Tables**:
   - `orders`: Order metadata with status, dates, total price
   - `order_items`: Individual line items with quantities
   - `sales`: Sales records linked to delivered orders (NEW)

2. **Data Files**:
   - `m5_product_mapping.csv`: M5 to product mappings
   - `orders_schedule.csv`: Customer order schedule
   - `orders_validation_report.md`: Comprehensive validation
   - `sales_trends.png`: Sales visualization (NEW)

3. **Metrics**:
   - ~200,000+ orders over 2 years
   - Average 1.5 orders/week/customer
   - 0.5% cancellation rate
   - Growing average order value: £38 → £43
   - Sales records = delivered order items count

## Next Steps
When ready to implement:
1. Start with Task 11 (Environment Setup)
2. Progress sequentially through tasks
3. Run validation after each major step
4. Adjust parameters if needed to meet targets