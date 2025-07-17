# Data Regeneration Plan

## Overview
Complete regeneration of grocery POC data with updated requirements for July 2025.

**Current Date**: July 14, 2025

## Requirements
1. ✓ Better sales distribution (already fixed in current DB)
2. Sales data generated up to today (July 14, 2025)
3. Orders generated up to July 20, 2025
4. July 15 orders must be in PICKED status
5. July 16-20 orders must be in FUTURE status
6. Generate 7-day forecast
7. All supply chain tables must be aligned with grocery_final.db

## Phase 1: Cleanup

### Files to Remove
```bash
# Remove old CSV files (except customers.csv)
rm src/data/daily_sales_summary.csv
rm src/data/m5_item_popularity.csv
rm src/data/m5_price_stats_gbp.csv
rm src/data/m5_product_mapping.csv
rm src/data/m5_unique_food_items.csv
rm src/data/monthly_sales_summary.csv
rm src/data/order_items.csv
rm src/data/orders.csv
rm src/data/orders_schedule.csv
rm src/data/product_sales_summary.csv
rm src/data/sales.csv

# Remove JSON summary files
rm src/data/customer_distribution_summary.json
rm src/data/final_database_info.json
rm src/data/m5_analysis_results.json
rm src/data/mapping_summary.json
rm src/data/order_generation_summary.json
rm src/data/sales_generation_summary.json
rm src/data/stock_generation_summary.json
rm src/data/supplier_schedules_summary.json
rm src/data/validation_report.json
rm src/data/product_purge_reference_summary.json

# Remove duplicate databases
rm src/scripts/grocery_final.db
rm src/scripts/orders.duckdb

# Remove old pickle files
rm src/data/*.pkl
```

### Files to Keep
- `src/data/products.duckdb` - Product catalog from web scraping
- `src/data/customers.csv` - Customer data from Mockaroo
- `src/data-extraction/data/products.csv` - Original scraped data

## Phase 2: Regenerate Base Data

### Script Execution Order
```bash
cd src/scripts

# 1. Environment setup
~/miniconda3/envs/grocery_poc/bin/python 01_environment_setup.py

# 2. Load M5 dataset
~/miniconda3/envs/grocery_poc/bin/python 02_m5_dataset_analysis.py

# 3. Product mapping
~/miniconda3/envs/grocery_poc/bin/python 03_product_mapping.py

# 4. Customer distribution
~/miniconda3/envs/grocery_poc/bin/python 04_customer_distribution.py

# 5. Order generation (with modifications)
~/miniconda3/envs/grocery_poc/bin/python 05_order_generation.py

# 6. Sales generation (with modifications)
~/miniconda3/envs/grocery_poc/bin/python 06_sales_generation.py

# 7. Final validation
~/miniconda3/envs/grocery_poc/bin/python 07_final_validation.py

# 8. Load to database
~/miniconda3/envs/grocery_poc/bin/python 08_load_to_database.py

# 11. Create final database
~/miniconda3/envs/grocery_poc/bin/python 11_create_final_database.py
```

### Key Modifications

#### Script 05 - Order Generation
- Extend order generation to July 20, 2025
- Ensure proper status assignment based on dates

#### Script 06 - Sales Generation
- Generate sales only up to July 14, 2025
- No sales for FUTURE or PICKED orders

#### Post-Generation Fix
```sql
-- Update July 15 orders to PICKED status
UPDATE orders 
SET orderStatus = 'PICKED' 
WHERE orderDate >= '2025-07-15' AND orderDate < '2025-07-16';

-- Ensure July 16-20 orders are FUTURE
UPDATE orders 
SET orderStatus = 'FUTURE' 
WHERE orderDate >= '2025-07-16';
```

## Phase 3: Generate Supply Chain Data

### Essential Scripts
```bash
# Create suppliers and forecasts
~/miniconda3/envs/grocery_poc/bin/python 15_create_suppliers_table.py

# Create supplier schedules
~/miniconda3/envs/grocery_poc/bin/python 17_generate_supplier_schedules.py

# Create product purge reference
~/miniconda3/envs/grocery_poc/bin/python 18_create_product_purge_reference.py

# Generate initial stock levels
~/miniconda3/envs/grocery_poc/bin/python 19_generate_stock_levels.py

# Create PO generation tables
~/miniconda3/envs/grocery_poc/bin/python 20_create_po_generation_tables.py

# Populate product ordering calendar
~/miniconda3/envs/grocery_poc/bin/python 22_populate_product_ordering_calendar.py

# Generate purchase orders
~/miniconda3/envs/grocery_poc/bin/python 23_generate_purchase_orders.py

# Generate live basket data
~/miniconda3/envs/grocery_poc/bin/python 25_generate_live_basket.py
```

## Phase 4: Generate 7-Day Forecast

The forecast generation is handled by script 15, which creates forecasts for:
- Historical data (past sales)
- Future 7 days from current date

## Phase 5: Export & Validate

### Export to Parquet
```bash
~/miniconda3/envs/grocery_poc/bin/python 31_export_all_tables_to_parquet.py
```

### Validation Checklist
- [ ] Sales evenly distributed (no single product spike)
- [ ] Sales records exist up to July 14, 2025
- [ ] Orders exist up to July 20, 2025
- [ ] July 15 orders have PICKED status
- [ ] July 16-20 orders have FUTURE status
- [ ] 7-day forecast available in forecasts table
- [ ] All supply chain tables populated
- [ ] Foreign key relationships intact

## DuckDB Date Handling Notes

### Important: DuckDB Date Functions
```sql
-- Extract date from timestamp
DATE_TRUNC('day', timestamp_column)

-- Compare dates
WHERE orderDate >= '2025-07-15' AND orderDate < '2025-07-16'

-- Format dates
STRFTIME(date_column, '%Y-%m-%d')

-- Current date
CURRENT_DATE

-- Date arithmetic
date_column + INTERVAL '7 days'
date_column - INTERVAL '2 days'

-- Extract parts
EXTRACT(YEAR FROM date_column)
EXTRACT(MONTH FROM date_column)
EXTRACT(DAY FROM date_column)
```

### Common Mistakes to Avoid
- ❌ `DATE(orderDate)` - This function doesn't exist in DuckDB
- ✓ `DATE_TRUNC('day', orderDate)` - Use this instead
- ✓ `CAST(orderDate AS DATE)` - Alternative approach

## Scripts to Remove (Not Essential)

These scripts are fixes or alternatives that won't be needed:
- 16_generate_latest_forecast.py (replaced by 15)
- 21_rename_to_product_ordering_calendar.py (one-time migration)
- 24_process_inbound_to_stock*.py (optional processing)
- 26_generate_forecasts_for_future.py (replaced by 15)
- 27-30_*.py (various fixes and updates)
- 32_create_stock_projection.py (creates views only)
- 33-34_fix_*.py (export fixes)
- 36*_fix_sales_distribution*.py (distribution already fixed)
- All test and check scripts

## Expected Final Database Structure

### Base Tables (6)
1. products
2. product_skus
3. customers
4. orders
5. order_items
6. sales

### Supply Chain Tables (10+)
1. suppliers
2. forecasts
3. supplier_schedules
4. product_purge_reference
5. stock
6. product_ordering_calendar
7. purchase_orders
8. inbound_deliveries
9. purge_log
10. live_basket

### Views (15+)
- customer_analytics
- product_catalog
- product_performance
- latest_forecasts
- forecast_accuracy
- supplier_delivery_calendar
- product_expiration_guide
- stock_summary
- stock_by_zone
- future_purges
- stock_availability
- pending_deliveries
- itb_summary
- ordering_opportunities
- stock_projection (if script 32 is run)