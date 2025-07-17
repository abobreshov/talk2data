# Databricks notebook source
# MAGIC %md
# MAGIC # Direct DuckDB to Databricks Upload
# MAGIC 
# MAGIC This notebook demonstrates how to directly read DuckDB files from Databricks using the DuckDB extension.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Option 1: Upload and Read DuckDB File Directly

# COMMAND ----------

# Install DuckDB in Databricks
%pip install duckdb

# COMMAND ----------

import duckdb
import pandas as pd

# Path to uploaded DuckDB file
duckdb_path = "/dbfs/FileStore/grocery_poc/grocery_final.db"  # Update this path

# Connect to DuckDB
conn = duckdb.connect(duckdb_path, read_only=True)

# Get list of tables
tables = conn.execute("""
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = 'main' 
    AND table_type = 'BASE TABLE'
""").fetchall()

print(f"Found {len(tables)} tables in DuckDB:")
for table in tables:
    print(f"  - {table[0]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Option 2: Convert Tables to Delta

# COMMAND ----------

catalog = "grocery_poc"
schema = "main"

# Create catalog and schema
spark.sql(f"CREATE CATALOG IF NOT EXISTS `{catalog}`")
spark.sql(f"USE CATALOG `{catalog}`")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{schema}`")

# COMMAND ----------

# Convert each DuckDB table to Delta
for table_name, in tables:
    try:
        # Read from DuckDB
        df_duck = conn.execute(f"SELECT * FROM {table_name}").df()
        
        # Convert to Spark DataFrame
        df_spark = spark.createDataFrame(df_duck)
        
        # Write as Delta table
        df_spark.write.mode("overwrite").saveAsTable(f"`{catalog}`.`{schema}`.`{table_name}`")
        
        count = spark.table(f"`{catalog}`.`{schema}`.`{table_name}`").count()
        print(f"✓ Converted {table_name}: {count:,} rows")
        
    except Exception as e:
        print(f"✗ Error converting {table_name}: {str(e)}")

# COMMAND ----------

# Close DuckDB connection
conn.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Data

# COMMAND ----------

# Check forecasts with predicted quantities
display(spark.sql(f"""
    SELECT 
        productId,
        target_date,
        predicted_quantity,
        confidence_lower,
        confidence_upper,
        model_name
    FROM `{catalog}`.`{schema}`.`forecasts`
    WHERE productId = '000000000000267186'
    AND target_date BETWEEN '2025-07-15' AND '2025-07-22'
    AND predicted_quantity IS NOT NULL
    ORDER BY target_date
"""))
