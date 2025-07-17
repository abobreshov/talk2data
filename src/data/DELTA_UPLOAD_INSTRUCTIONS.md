# Delta Lake Upload Instructions for Databricks

## Overview

This approach exports DuckDB tables directly to Delta Lake format, which Databricks can read natively.

## Method 1: Upload Delta Tables (Recommended)

### 1. Upload Delta Tables to DBFS

```bash
# Upload the entire delta directory
databricks fs cp -r ./delta_export dbfs:/FileStore/grocery_poc/delta/ --overwrite
```

### 2. Register Tables in Databricks

Run `databricks_register_delta_tables.sql` in Databricks SQL Editor to:
- Create catalog and schema
- Register all Delta tables as external tables
- Compute statistics

### 3. Verify

```sql
USE CATALOG grocery_poc;
USE SCHEMA main;
SHOW TABLES;

-- Check forecast data
SELECT COUNT(*) 
FROM forecasts 
WHERE predicted_quantity IS NOT NULL;
```

## Method 2: Direct DuckDB Upload

### 1. Upload DuckDB File

```bash
# Upload the DuckDB file directly
databricks fs cp ../data/grocery_final.db dbfs:/FileStore/grocery_poc/grocery_final.db
```

### 2. Use Direct Upload Notebook

Import and run `direct_duckdb_upload.py` which:
- Installs DuckDB in Databricks
- Reads tables directly from DuckDB file
- Converts to Delta tables in Unity Catalog

## Advantages of Each Method

### Delta Export Method
- ✓ Faster for large datasets
- ✓ No need to install DuckDB in Databricks
- ✓ Tables are immediately queryable
- ✓ Better performance for analytics

### Direct DuckDB Method
- ✓ Single file upload
- ✓ No intermediate export needed
- ✓ Preserves exact DuckDB schema
- ✓ Good for smaller databases

## Performance Tips

1. For large tables, consider partitioning:
   ```sql
   ALTER TABLE sales 
   SET TBLPROPERTIES ('delta.targetFileSize' = '128MB');
   
   OPTIMIZE sales;
   ```

2. Enable auto-optimization:
   ```sql
   ALTER TABLE sales 
   SET TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true');
   ```

3. Z-order frequently filtered columns:
   ```sql
   OPTIMIZE forecasts 
   ZORDER BY (productId, target_date);
   ```

## Troubleshooting

### Issue: Path not found
Ensure DBFS path is correct and files are uploaded

### Issue: Schema mismatch
Delta tables preserve DuckDB schema exactly - no mapping needed

### Issue: Performance
Run OPTIMIZE on large tables after initial load
