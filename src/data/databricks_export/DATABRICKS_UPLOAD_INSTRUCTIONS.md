# Databricks Unity Catalog Upload Instructions

## Prerequisites

1. Databricks workspace with Unity Catalog enabled
2. Databricks CLI installed and configured
3. Appropriate permissions to create catalogs and tables
4. Storage location accessible from Databricks (DBFS, S3, Azure Blob, etc.)

## Step-by-Step Upload Process

### 1. Upload Parquet Files to Databricks

Using Databricks CLI:
```bash
# Create directory in DBFS
databricks fs mkdirs dbfs:/FileStore/grocery_poc

# Upload all Parquet files
databricks fs cp -r ./parquet_export dbfs:/FileStore/grocery_poc/ --overwrite
```

Or using Databricks UI:
1. Go to Data > DBFS > FileStore
2. Create folder "grocery_poc"
3. Upload all .parquet files from parquet_export/

### 2. Execute SQL Script

Option A - Using SQL Editor:
1. Open Databricks SQL Editor
2. Copy contents of `databricks_upload_script.sql`
3. Execute step by step or all at once

Option B - Using Notebook:
1. Import `databricks_upload_notebook.py` to Databricks
2. Run all cells in order

### 3. Verify Upload

Run these checks:
```sql
-- Check catalog exists
SHOW CATALOGS LIKE 'grocery_poc';

-- Check all tables
USE CATALOG grocery_poc;
USE SCHEMA main;
SHOW TABLES;

-- Verify row counts match
SELECT 'forecasts' as table_name, COUNT(*) as rows FROM forecasts
UNION ALL
SELECT 'sales', COUNT(*) FROM sales
UNION ALL
SELECT 'orders', COUNT(*) FROM orders;
```

## Table Dependencies

Upload tables in this order to respect foreign keys:
1. customers, products, suppliers
2. product_skus
3. orders
4. order_items, sales
5. All other tables

## Data Types Mapping

| DuckDB Type | Databricks Type |
|-------------|-----------------|
| VARCHAR | STRING |
| INTEGER | INT |
| DECIMAL(10,2) | DECIMAL(10,2) |
| DATE | DATE |
| TIMESTAMP | TIMESTAMP |

## Troubleshooting

### Issue: Schema mismatch
- Solution: Set `mergeSchema = true` in COPY INTO options

### Issue: Foreign key constraints fail
- Solution: Load tables in dependency order
- Alternative: Add constraints after all data is loaded

### Issue: Large file upload timeout
- Solution: Split large tables into smaller Parquet files
- Use: `df.coalesce(10).write.parquet()` to create multiple files

## Performance Optimization

After upload:
1. Run OPTIMIZE on large tables
2. Enable auto-optimize for frequently updated tables
3. Consider Z-ordering on commonly filtered columns:
   ```sql
   OPTIMIZE sales ZORDER BY (productId, saleDate);
   ```

## Security Considerations

1. Use Unity Catalog access controls
2. Grant appropriate permissions:
   ```sql
   GRANT SELECT ON TABLE grocery_poc.main.sales TO `data_analysts`;
   ```
3. Enable column-level security for sensitive data

## Next Steps

1. Create views for common queries
2. Set up scheduled jobs for data refresh
3. Build dashboards using Databricks SQL
4. Configure alerts for data quality monitoring
