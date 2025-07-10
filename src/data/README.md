# Data Directory

This directory contains all data files for the grocery POC system, including databases, CSV files, and exported data.

## Database Files

### Primary Databases
- `products.duckdb` - Product catalog database containing:
  - `products` table - 2,501 products with categories from Aldi scraper
  - `product_skus` table - 5,039 Product-SKU mappings (1-3 SKUs per product)

- `orders.duckdb` - Order synthesis intermediate database

- `grocery_final.db` - Consolidated final DuckDB database containing:
  - `products` - 2,501 products with productId as primary key
  - `product_skus` - 5,039 SKU mappings
  - `customers` - 1,000 customer records (with city field)
  - `orders` - 131,268 orders over 2 years
  - `order_items` - 1.7M order items
  - `sales` - 2.8M sales records
  - Views: `customer_analytics`, `product_catalog`, `product_performance`

## CSV Files

### Input Data
- `customers.csv` - 1,000 customer records (id, first_name, last_name, email, gender, address, city, postcode)
  - Generated using Mockaroo.com - see `/docs/data-generation/mockaroo-customers.md` for instructions

### M5 Dataset Files
- `m5_unique_food_items.csv` - 1,437 unique FOOD items from Walmart M5 dataset
- `m5_item_popularity.csv` - Item popularity rankings
- `m5_price_stats_gbp.csv` - Price statistics in GBP
- `m5_product_mapping.csv` - Mapping between M5 items and our products

### Generated Data
- `orders_schedule.csv` - Customer order schedules
- `orders.csv` - 131,268 generated orders
- `order_items.csv` - 1.7M order items
- `sales.csv` - 2.8M sales records

### Summary Files
- `daily_sales_summary.csv` - Daily sales aggregations
- `monthly_sales_summary.csv` - Monthly sales aggregations
- `product_sales_summary.csv` - Product-level sales summary

## Parquet Export Directory

The `parquet/` subdirectory contains all tables and views exported to Parquet format:
- All 6 tables as compressed Parquet files
- All 3 views as materialized Parquet files
- `export_summary.json` - Export metadata and statistics
- Total size: ~70MB (compressed using SNAPPY)

## JSON Summary Files

- `customer_distribution_summary.json` - Customer order distribution statistics
- `order_generation_summary.json` - Order generation process summary
- `sales_generation_summary.json` - Sales generation statistics
- `mapping_summary.json` - Product mapping coverage (54.6% of products used)
- `final_database_info.json` - Final database structure and counts
- `validation_report.json` - Data validation results

## Pickle Files

- `customer_behaviors.pkl` - Customer shopping behavior patterns
- `m5_product_lookup.pkl` - Bidirectional M5-to-product lookup dictionary

## Visualization Files

- `m5_analysis_plots.png` - M5 dataset analysis visualizations
- `product_mapping_distribution.png` - Product mapping distribution charts
- `order_generation_analysis.png` - Order generation statistics
- `sales_analysis.png` - Sales analysis visualizations

## Usage Examples

### Connecting to DuckDB databases:
```python
import duckdb
from pathlib import Path

# Connect to final database
conn = duckdb.connect('src/data/grocery_final.db')

# Query example
df = conn.execute("SELECT * FROM products LIMIT 10").df()
```

### Reading Parquet files:
```python
import pandas as pd
import duckdb

# With pandas (requires pyarrow)
df = pd.read_parquet('src/data/parquet/products.parquet')

# With DuckDB
conn = duckdb.connect()
df = conn.execute("SELECT * FROM read_parquet('src/data/parquet/products.parquet')").df()
```

## Important Notes

1. **Database Format**: All `.db` and `.duckdb` files are DuckDB databases
2. **Price Format**: Prices are stored in pence (integers), use `/100` for GBP
3. **Product IDs**: 18-character format, padded with zeros
4. **Customer Updates**: `customers` table now includes `city` field, `ip_address` removed
5. **Product Coverage**: Order synthesis uses 1,366 products (54.6% of 2,501 total)

## Git Ignore

Large files are excluded from version control:
- `*.duckdb`, `*.db` - Database files
- `*.parquet` - Parquet files
- `*.csv` (large generated files)
- `*.pkl` - Pickle files