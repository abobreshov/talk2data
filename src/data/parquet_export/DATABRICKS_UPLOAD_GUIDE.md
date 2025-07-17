# Databricks Upload Guide

## Export Information
- Export Date: 2025-07-16 17:54:34
- Total Tables: 16
- Total Size: 84.68 MB

## Files to Upload

All files are located in: `/home/abobreshov/work/dataart/talk2data/grocery_poc/src/data/../data/parquet_export`

### Tables (16 files)
- `customers.parquet` (0.1 MB) - 1,000 rows
- `forecasts.parquet` (0.4 MB) - 16,569 rows
- `inbound_deliveries.parquet` (1.56 MB) - 32,826 rows
- `live_basket.parquet` (0.0 MB) - 16 rows
- `order_items.parquet` (30.42 MB) - 2,133,923 rows
- `orders.parquet` (1.62 MB) - 132,007 rows
- `product_ordering_calendar.parquet` (0.53 MB) - 47,634 rows
- `product_purge_reference.parquet` (0.0 MB) - 47 rows
- `product_skus.parquet` (0.16 MB) - 12,526 rows
- `products.parquet` (0.07 MB) - 2,501 rows
- `purchase_orders.parquet` (0.61 MB) - 32,773 rows
- `purge_log.parquet` (0.0 MB) - 0 rows
- `sales.parquet` (48.98 MB) - 3,437,419 rows
- `stock.parquet` (0.09 MB) - 4,998 rows
- `supplier_schedules.parquet` (0.13 MB) - 30,613 rows
- `suppliers.parquet` (0.01 MB) - 121 rows

### Metadata
- `export_summary.json` - Export metadata and statistics

## Upload Steps

### Option 1: Using Databricks CLI
```bash
# Upload all parquet files
databricks fs cp -r /home/abobreshov/work/dataart/talk2data/grocery_poc/src/data/../data/parquet_export/*.parquet dbfs:/FileStore/grocery_poc/

# Upload summary
databricks fs cp /home/abobreshov/work/dataart/talk2data/grocery_poc/src/data/../data/parquet_export/export_summary.json dbfs:/FileStore/grocery_poc/
```

### Option 2: Using Databricks UI
1. Go to Data → Add Data → Upload File
2. Select all .parquet files from /home/abobreshov/work/dataart/talk2data/grocery_poc/src/data/../data/parquet_export
3. Upload to desired location

## Create Delta Tables in Databricks

```python
# In Databricks notebook
import json

# Set base path
base_path = "dbfs:/FileStore/grocery_poc"

# Create database
spark.sql("CREATE DATABASE IF NOT EXISTS grocery_poc")
spark.sql("USE grocery_poc")

# Load export summary
with open(f"{base_path}/export_summary.json", "r") as f:
    export_info = json.load(f)

# Create Delta tables from Parquet files
for table_name in export_info['tables'].keys():
    print(f"Loading suppliers...")
    
    # Read Parquet
    df = spark.read.parquet(f"{base_path}/{table_name}.parquet")
    
    # Write as Delta table
    df.write.mode("overwrite").saveAsTable(f"grocery_poc.{table_name}")
    
    # Show count
    count = spark.sql(f"SELECT COUNT(*) FROM grocery_poc.{table_name}").collect()[0][0]
    print(f"✓ Created {table_name} with {count:,} rows")
```

## Verify Data

```python
# Check all tables
tables = spark.sql("SHOW TABLES IN grocery_poc").collect()
print(f"\nTotal tables: 16")
for table in tables:
    print(f"  - {table.tableName}")

# Sample queries
print("\n=== Sample Data ===")
spark.sql("SELECT * FROM grocery_poc.products LIMIT 5").show()
spark.sql("SELECT COUNT(*) as order_count FROM grocery_poc.orders").show()
```
