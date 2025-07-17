# Databricks Upload Guide

## Files to Upload

All Parquet files are located in: `src/data/parquet/`

### Core Tables (7 files)
- `customers.parquet` (98KB) - 1,000 customer records
- `products.parquet` (71KB) - 2,501 product records  
- `product_skus.parquet` (51KB) - 5,039 SKU mappings
- `orders.parquet` (1.6MB) - 131,268 order records
- `order_items.parquet` (27MB) - 1.7M order items
- `sales.parquet` (42MB) - 2.8M sales records
- `forecasts.parquet` (6.0MB) - 112,504 forecast records

### Analytical Views (5 files)
- `customer_analytics_view.parquet` (141KB)
- `product_catalog_view.parquet` (209KB)
- `product_performance_view.parquet` (112KB)
- `forecast_accuracy_view.parquet` (965KB)
- `latest_forecasts_view.parquet` (876KB)

**Total Size: ~78MB** (compressed)

## Upload Steps

### 1. Upload to DBFS (Databricks File System)

```bash
# Using Databricks CLI
databricks fs cp -r ./src/data/parquet/ dbfs:/FileStore/grocery_poc/

# Or upload via UI:
# Data > Add Data > Upload File
```

### 2. Create Delta Tables from Parquet

```python
# In Databricks notebook

# Set the base path
base_path = "dbfs:/FileStore/grocery_poc"

# Create database
spark.sql("CREATE DATABASE IF NOT EXISTS grocery_poc")
spark.sql("USE grocery_poc")

# Load each Parquet file as Delta table
tables = [
    "customers", "products", "product_skus", 
    "orders", "order_items", "sales", "forecasts"
]

for table in tables:
    # Read Parquet
    df = spark.read.parquet(f"{base_path}/{table}.parquet")
    
    # Write as Delta
    df.write.mode("overwrite").saveAsTable(f"grocery_poc.{table}")
    
    print(f"✓ Created Delta table: {table}")
```

### 3. Create Views

```python
# Create analytical views
views = [
    "customer_analytics_view",
    "product_catalog_view", 
    "product_performance_view",
    "forecast_accuracy_view",
    "latest_forecasts_view"
]

for view in views:
    # Read the view parquet
    df = spark.read.parquet(f"{base_path}/{view}.parquet")
    
    # Create temp view
    view_name = view.replace("_view", "")
    df.createOrReplaceTempView(view_name)
    
    print(f"✓ Created view: {view_name}")
```

### 4. Verify Data

```python
# Check table counts
for table in tables:
    count = spark.sql(f"SELECT COUNT(*) FROM grocery_poc.{table}").collect()[0][0]
    print(f"{table}: {count:,} records")

# Sample query
spark.sql("""
    SELECT 
        p.name as product_name,
        p.category,
        SUM(s.quantity) as total_quantity,
        SUM(s.totalPrice) as revenue
    FROM grocery_poc.sales s
    JOIN grocery_poc.products p ON s.productId = p.productId
    GROUP BY p.name, p.category
    ORDER BY revenue DESC
    LIMIT 10
""").show()
```

## Alternative: Direct Parquet Usage

You can also query Parquet files directly without converting to Delta:

```python
# Register Parquet files as temporary views
spark.read.parquet(f"{base_path}/products.parquet").createOrReplaceTempView("products")
spark.read.parquet(f"{base_path}/sales.parquet").createOrReplaceTempView("sales")

# Query directly
spark.sql("""
    SELECT * FROM products WHERE category = 'Fresh Food' LIMIT 10
""").show()
```

## Schema Information

### Key Relationships
- `product_skus.productId` → `products.productId`
- `orders.customerId` → `customers.customerId`  
- `order_items.orderId` → `orders.orderId`
- `order_items.productId` → `products.productId`
- `sales.orderId` → `orders.orderId`
- `sales.productId` → `products.productId`
- `sales.customerId` → `customers.customerId`
- `forecasts.productId` → `products.productId`

### Important Notes
1. **Price Format**: Prices are stored in pence (integers). Use `price_gbp` column or divide by 100
2. **Date Columns**: All dates are already in DATE/TIMESTAMP format
3. **Product IDs**: 18-character strings (zero-padded)
4. **Forecast Data**: Includes 7-day forecasts with confidence intervals

## Sample Forecasting Query

```python
# Get latest forecast for top products
spark.sql("""
    SELECT 
        p.name as product_name,
        f.forecast_date,
        f.target_date,
        f.predicted_quantity,
        f.confidence_lower,
        f.confidence_upper
    FROM grocery_poc.forecasts f
    JOIN grocery_poc.products p ON f.productId = p.productId
    WHERE f.forecast_date = (SELECT MAX(forecast_date) FROM grocery_poc.forecasts)
    AND f.forecast_horizon <= 7
    ORDER BY f.predicted_quantity DESC
    LIMIT 20
""").show()
```