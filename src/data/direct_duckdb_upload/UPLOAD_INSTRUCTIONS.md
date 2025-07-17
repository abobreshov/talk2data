# Direct DuckDB to Databricks Upload Instructions

## Overview

This approach uploads the DuckDB file directly to Databricks and reads it using the DuckDB Python library.

## Prerequisites

1. Databricks workspace with Unity Catalog enabled
2. Databricks CLI installed and configured
3. Cluster with Unity Catalog support

## Step-by-Step Process

### 1. Upload DuckDB File to DBFS

Using Databricks CLI:
```bash
# Create directory
databricks fs mkdirs dbfs:/FileStore/grocery_poc

# Upload the DuckDB file
databricks fs cp grocery_final.db dbfs:/FileStore/grocery_poc/grocery_final.db
```

Or using Databricks UI:
1. Go to Data > DBFS > FileStore
2. Create folder "grocery_poc"
3. Upload grocery_final.db file

### 2. Import and Run the Notebook

1. In Databricks workspace, click "Import"
2. Upload `direct_duckdb_upload_notebook.py`
3. Attach to a Unity Catalog enabled cluster
4. Run all cells

The notebook will:
- Install DuckDB in the cluster
- Read tables directly from the .db file
- Convert to Delta tables in Unity Catalog
- Preserve all data including forecasts with predicted_quantity values

### 3. Verify the Upload

After running the notebook, verify:

```sql
-- Check catalog
USE CATALOG grocery_poc;
SHOW SCHEMAS;

-- Check tables
USE SCHEMA main;
SHOW TABLES;

-- Verify forecast data
SELECT COUNT(*) 
FROM forecasts 
WHERE predicted_quantity IS NOT NULL
AND target_date BETWEEN '2025-07-15' AND '2025-07-22';
```

## Advantages of Direct Upload

1. **Single File**: Upload one .db file instead of multiple Parquet files
2. **Data Integrity**: Preserves exact DuckDB schema and data types
3. **Simplicity**: No intermediate export/import steps
4. **Flexibility**: Can query DuckDB directly or convert to Delta

## Performance Considerations

For large databases (>1GB):
1. Use a larger cluster for the initial load
2. Enable auto-optimize on frequently queried tables
3. Consider partitioning large tables like sales

## Troubleshooting

### Issue: DuckDB not found
Solution: Ensure the file path is correct and file is uploaded to DBFS

### Issue: Memory errors
Solution: Use a larger cluster or process tables in batches

### Issue: Data type mismatches (e.g., DECIMAL vs DOUBLE)
Solution: The notebook handles type conversion automatically using temporary views and SQL CAST operations. This approach avoids Arrow optimization issues and DBFS restrictions

## Next Steps

After successful upload:
1. Create additional views for analytics
2. Set up scheduled refreshes if needed
3. Build dashboards using Databricks SQL
4. Grant appropriate permissions to users

## Alternative: Using Parquet Export

If you prefer the Parquet approach:
1. Use script `31_export_all_tables_to_parquet.py`
2. Upload Parquet files to DBFS
3. Use the SQL script from `databricks_export/`

Both methods preserve all data including forecast predictions.
