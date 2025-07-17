# Databricks notebook source
# MAGIC %md
# MAGIC # Grocery POC Database Upload to Unity Catalog
# MAGIC 
# MAGIC This notebook uploads the DuckDB grocery_final.db to Databricks Unity Catalog.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

catalog = "grocery_poc"
schema = "main"
source_path = "dbfs:/FileStore/grocery_poc"  # Update this path

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create Catalog and Schema

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS `{catalog}`")
spark.sql(f"USE CATALOG `{catalog}`")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{schema}`")
spark.sql(f"USE SCHEMA `{schema}`")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Upload Parquet Files
# MAGIC 
# MAGIC Before proceeding, upload all Parquet files from `parquet_export/` to DBFS:
# MAGIC 
# MAGIC ```bash
# MAGIC databricks fs cp -r /local/path/to/parquet_export dbfs:/FileStore/grocery_poc/
# MAGIC ```

# COMMAND ----------

# Verify files are uploaded
dbutils.fs.ls(source_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create Tables and Load Data

# COMMAND ----------

# Table creation order (respecting dependencies)
table_order = [
    'customers', 'products', 'suppliers', 'product_skus',
    'orders', 'order_items', 'sales', 'stock', 'forecasts',
    'supplier_schedules', 'product_ordering_calendar', 
    'purchase_orders', 'inbound_deliveries', 'live_basket',
    'product_purge_reference', 'purge_log'
]

# Load each table
for table_name in table_order:
    try:
        # Read Parquet file
        df = spark.read.parquet(f"{source_path}/{table_name}.parquet")
        
        # Write to Unity Catalog table
        df.write.mode("overwrite").saveAsTable(f"`{catalog}`.`{schema}`.`{table_name}`")
        
        # Get row count
        count = spark.table(f"`{catalog}`.`{schema}`.`{table_name}`").count()
        print(f"✓ Loaded {table_name}: {count:,} rows")
        
    except Exception as e:
        print(f"✗ Error loading {table_name}: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Add Table Properties and Optimize

# COMMAND ----------

for table_name in table_order:
    try:
        # Add table comment
        spark.sql(f"""
            ALTER TABLE `{catalog}`.`{schema}`.`{table_name}`
            SET TBLPROPERTIES ('comment' = 'Imported from DuckDB grocery_final.db')
        """)
        
        # Optimize table
        spark.sql(f"OPTIMIZE `{catalog}`.`{schema}`.`{table_name}`")
        
        # Compute statistics
        spark.sql(f"ANALYZE TABLE `{catalog}`.`{schema}`.`{table_name}` COMPUTE STATISTICS")
        
        print(f"✓ Optimized {table_name}")
        
    except Exception as e:
        print(f"✗ Error optimizing {table_name}: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Verify Data Integrity

# COMMAND ----------

# Check row counts
print("Table Row Counts:")
print("-" * 40)

total_rows = 0
for table_name in table_order:
    try:
        count = spark.table(f"`{catalog}`.`{schema}`.`{table_name}`").count()
        total_rows += count
        print(f"{table_name:30} {count:>10,}")
    except:
        print(f"{table_name:30} {'ERROR':>10}")

print("-" * 40)
print(f"{'TOTAL':30} {total_rows:>10,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Sample Queries

# COMMAND ----------

# Sample forecast data
display(spark.sql(f"""
    SELECT * 
    FROM `{catalog}`.`{schema}`.`forecasts`
    WHERE productId = '267186'
    AND target_date BETWEEN '2025-07-15' AND '2025-07-22'
    ORDER BY target_date
    LIMIT 10
"""))

# COMMAND ----------

# Product performance
display(spark.sql(f"""
    SELECT 
        p.name as product_name,
        COUNT(DISTINCT s.orderId) as order_count,
        SUM(s.quantity) as total_quantity,
        SUM(s.totalPrice) / 100.0 as revenue_gbp
    FROM `{catalog}`.`{schema}`.`sales` s
    JOIN `{catalog}`.`{schema}`.`products` p ON s.productId = p.productId
    GROUP BY p.name
    ORDER BY revenue_gbp DESC
    LIMIT 20
"""))
