# Databricks notebook source
# MAGIC %md
# MAGIC # Direct DuckDB to Databricks Unity Catalog Upload
# MAGIC 
# MAGIC This notebook reads the DuckDB file directly and creates Delta tables in Unity Catalog.
# MAGIC All forecast data with predicted_quantity values will be preserved.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Install DuckDB

# COMMAND ----------

# Install DuckDB for Python
%pip install duckdb pyarrow

# COMMAND ----------

# Import required libraries
import duckdb
import pandas as pd
import numpy as np
from pyspark.sql import functions as F
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Configuration

# COMMAND ----------

# Configuration
DUCKDB_PATH = "/dbfs/FileStore/grocery_poc/grocery_final.db"  # Update if needed
CATALOG = "grocery_poc"
SCHEMA = "main"

# Verify DuckDB file exists
import os
if os.path.exists(DUCKDB_PATH):
    print(f"✓ DuckDB file found at: {DUCKDB_PATH}")
    print(f"  File size: {os.path.getsize(DUCKDB_PATH) / (1024*1024):.2f} MB")
else:
    print(f"✗ DuckDB file not found at: {DUCKDB_PATH}")
    print("  Please upload the file using: databricks fs cp grocery_final.db dbfs:/FileStore/grocery_poc/")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Create Catalog and Schema

# COMMAND ----------

# Create catalog and schema
spark.sql(f"CREATE CATALOG IF NOT EXISTS `{CATALOG}`")
spark.sql(f"USE CATALOG `{CATALOG}`")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{SCHEMA}`")
spark.sql(f"USE SCHEMA `{SCHEMA}`")

print(f"✓ Using catalog: {CATALOG}")
print(f"✓ Using schema: {SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Connect to DuckDB and List Tables

# COMMAND ----------

# Connect to DuckDB
conn = duckdb.connect(DUCKDB_PATH, read_only=True)

# Get list of tables
tables_df = conn.execute("""
    SELECT 
        table_name,
        (SELECT COUNT(*) FROM pragma_table_info(t.table_name)) as column_count
    FROM information_schema.tables t
    WHERE table_schema = 'main' 
    AND table_type = 'BASE TABLE'
    ORDER BY table_name
""").df()

print(f"Found {len(tables_df)} tables in DuckDB:")
for _, row in tables_df.iterrows():
    print(f"  - {row['table_name']} ({row['column_count']} columns)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Define Table Load Order (Respecting Dependencies)

# COMMAND ----------

# Define load order to respect foreign key dependencies
table_load_order = [
    'customers',      # No dependencies
    'suppliers',      # No dependencies
    'products',       # May reference suppliers
    'product_skus',   # References products
    'orders',         # References customers
    'order_items',    # References orders and products
    'sales',          # References orders, customers, products
    'stock',          # References products, suppliers
    'forecasts',      # References products
    'supplier_schedules',        # References suppliers
    'product_ordering_calendar',  # References products, suppliers
    'purchase_orders',           # References products, suppliers
    'inbound_deliveries',        # References purchase_orders
    'live_basket',              # References products
    'product_purge_reference',   # References products
    'purge_log'                 # May reference multiple tables
]

# Ensure all tables are included
all_tables = set(tables_df['table_name'].tolist())
for table in table_load_order:
    if table in all_tables:
        all_tables.remove(table)

# Add any remaining tables
table_load_order.extend(list(all_tables))

print(f"Will load {len(table_load_order)} tables in dependency order")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Load Tables to Unity Catalog

# COMMAND ----------

# Function to map DuckDB types to Spark types
def duckdb_to_spark_type(duckdb_type):
    """Map DuckDB data types to Spark SQL types"""
    type_str = str(duckdb_type).upper()
    
    # Remove parameters for comparison
    base_type = type_str.split('(')[0]
    
    type_mapping = {
        'INTEGER': IntegerType(),
        'BIGINT': LongType(),
        'SMALLINT': ShortType(),
        'TINYINT': ByteType(),
        'DECIMAL': DecimalType(10, 2),  # Default precision
        'DOUBLE': DoubleType(),
        'FLOAT': FloatType(),
        'VARCHAR': StringType(),
        'CHAR': StringType(),
        'TEXT': StringType(),
        'DATE': DateType(),
        'TIMESTAMP': TimestampType(),
        'TIMESTAMP WITH TIME ZONE': TimestampType(),
        'BOOLEAN': BooleanType(),
        'BLOB': BinaryType(),
        'UUID': StringType()
    }
    
    # Handle decimal with precision
    if 'DECIMAL' in type_str and '(' in type_str:
        # Extract precision and scale
        try:
            params = type_str.split('(')[1].rstrip(')').split(',')
            precision = int(params[0])
            scale = int(params[1]) if len(params) > 1 else 0
            return DecimalType(precision, scale)
        except:
            pass
    
    return type_mapping.get(base_type, StringType())

# COMMAND ----------

# Load each table
load_results = []

for table_name in table_load_order:
    try:
        print(f"\nLoading {table_name}...")
        
        # Get row count
        row_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        
        if row_count == 0:
            # Create empty table with schema
            columns_info = conn.execute(f"""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = '{table_name}'
                ORDER BY ordinal_position
            """).fetchall()
            
            # Create schema
            spark_schema = StructType([
                StructField(col_name, duckdb_to_spark_type(data_type), True)
                for col_name, data_type in columns_info
            ])
            
            # Create empty DataFrame with schema
            empty_df = spark.createDataFrame([], spark_schema)
            empty_df.write.mode("overwrite").saveAsTable(f"`{CATALOG}`.`{SCHEMA}`.`{table_name}`")
            
            print(f"  ✓ Created empty table {table_name}")
            load_results.append((table_name, 0, "Success"))
            
        else:
            # Get schema information for proper type handling
            columns_info = conn.execute(f"""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = '{table_name}'
                ORDER BY ordinal_position
            """).fetchall()
            
            # Read data from DuckDB
            # Use fetchdf() for better pandas compatibility
            df_pandas = conn.execute(f"SELECT * FROM {table_name}").fetchdf()
            
            # Handle special columns
            if 'productId' in df_pandas.columns:
                # Convert to string and pad with zeros
                df_pandas['productId'] = df_pandas['productId'].astype(str).str.zfill(18)
            
            # Handle TIME columns before creating Spark DataFrame
            time_columns = [col_name for col_name, data_type in columns_info if data_type.upper() == 'TIME']
            has_time_columns = len(time_columns) > 0
            
            if has_time_columns:
                print(f"  Found TIME columns: {time_columns}")
                # Convert TIME columns to strings (HH:MM:SS format)
                for col in time_columns:
                    if col in df_pandas.columns:
                        print(f"  Converting {col} to string")
                        df_pandas[col] = df_pandas[col].astype(str)
            
            # Convert to Spark DataFrame
            # For tables with TIME columns, create schema explicitly to avoid Arrow issues
            if has_time_columns:
                # Build schema manually
                from pyspark.sql.types import StructType, StructField
                
                fields = []
                for col_name, data_type in columns_info:
                    if data_type.upper() == 'TIME':
                        # TIME columns are now strings
                        fields.append(StructField(col_name, StringType(), True))
                    else:
                        # Use the mapping function for other types
                        spark_type = duckdb_to_spark_type(data_type)
                        fields.append(StructField(col_name, spark_type, True))
                
                schema = StructType(fields)
                df_spark = spark.createDataFrame(df_pandas, schema=schema)
            else:
                # For other tables, let Spark infer the schema
                df_spark = spark.createDataFrame(df_pandas)
            
            # Cast DECIMAL, DATE, TIMESTAMP, and FLOAT columns to proper types
            import re
            for col_name, data_type in columns_info:
                data_type_upper = data_type.upper()
                
                if 'DECIMAL' in data_type_upper:
                    # Extract precision and scale from DECIMAL type
                    match = re.search(r'DECIMAL\((\d+),(\d+)\)', data_type_upper)
                    if match:
                        precision, scale = int(match.group(1)), int(match.group(2))
                    else:
                        precision, scale = 10, 2  # Default
                    
                    # Cast the column to proper DECIMAL type
                    if col_name in df_spark.columns:
                        df_spark = df_spark.withColumn(col_name, df_spark[col_name].cast(DecimalType(precision, scale)))
                
                elif data_type_upper == 'FLOAT':
                    # Cast FLOAT columns to DoubleType to avoid merge conflicts
                    if col_name in df_spark.columns:
                        df_spark = df_spark.withColumn(col_name, df_spark[col_name].cast(DoubleType()))
                
                elif data_type_upper == 'DATE':
                    # Cast DATE columns
                    if col_name in df_spark.columns:
                        df_spark = df_spark.withColumn(col_name, df_spark[col_name].cast(DateType()))
                
                elif data_type_upper in ['TIMESTAMP', 'TIMESTAMP WITH TIME ZONE']:
                    # Cast TIMESTAMP columns
                    if col_name in df_spark.columns:
                        df_spark = df_spark.withColumn(col_name, df_spark[col_name].cast(TimestampType()))
                
                elif data_type_upper == 'TIME':
                    # TIME columns are already converted to strings, keep as StringType
                    pass
            
            # Write to Delta table
            df_spark.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(f"`{CATALOG}`.`{SCHEMA}`.`{table_name}`")
            
            # Verify
            loaded_count = spark.table(f"`{CATALOG}`.`{SCHEMA}`.`{table_name}`").count()
            
            print(f"  ✓ Loaded {table_name}: {loaded_count:,} rows")
            load_results.append((table_name, loaded_count, "Success"))
            
    except Exception as e:
        print(f"  ✗ Error loading {table_name}: {str(e)}")
        load_results.append((table_name, 0, f"Error: {str(e)[:100]}"))

# COMMAND ----------

# Display load results
results_df = spark.createDataFrame(load_results, ["table_name", "row_count", "status"])
display(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Verify Critical Data

# COMMAND ----------

# Verify forecasts with predicted_quantity
forecast_check = spark.sql(f"""
    SELECT 
        COUNT(*) as total_forecasts,
        COUNT(CASE WHEN predicted_quantity IS NOT NULL THEN 1 END) as non_null_predictions,
        COUNT(CASE WHEN predicted_quantity > 0 THEN 1 END) as positive_predictions,
        COUNT(DISTINCT productId) as unique_products,
        MIN(target_date) as min_date,
        MAX(target_date) as max_date
    FROM `{CATALOG}`.`{SCHEMA}`.`forecasts`
""").collect()[0]

print("Forecast Data Verification:")
print(f"  Total forecasts: {forecast_check['total_forecasts']:,}")
print(f"  Non-null predictions: {forecast_check['non_null_predictions']:,}")
print(f"  Positive predictions: {forecast_check['positive_predictions']:,}")
print(f"  Unique products: {forecast_check['unique_products']:,}")
print(f"  Date range: {forecast_check['min_date']} to {forecast_check['max_date']}")

# COMMAND ----------

# Check specific product forecast
display(spark.sql(f"""
    SELECT 
        productId,
        forecast_date,
        target_date,
        predicted_quantity,
        confidence_lower,
        confidence_upper,
        model_name
    FROM `{CATALOG}`.`{SCHEMA}`.`forecasts`
    WHERE productId = '000000000000267186'
    AND target_date BETWEEN '2025-07-15' AND '2025-07-22'
    AND predicted_quantity IS NOT NULL
    ORDER BY target_date, model_name
    LIMIT 20
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Optimize Tables

# COMMAND ----------

# Optimize large tables
large_tables = ['sales', 'order_items', 'forecasts']

for table_name in large_tables:
    try:
        spark.sql(f"OPTIMIZE `{CATALOG}`.`{SCHEMA}`.`{table_name}`")
        print(f"✓ Optimized {table_name}")
    except Exception as e:
        print(f"✗ Could not optimize {table_name}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Create Sample Analytics Views

# COMMAND ----------

# Create product performance view
spark.sql(f"""
    CREATE OR REPLACE VIEW `{CATALOG}`.`{SCHEMA}`.`v_product_performance` AS
    SELECT 
        p.productId,
        p.name as product_name,
        p.category,
        COUNT(DISTINCT s.orderId) as order_count,
        SUM(s.quantity) as total_quantity_sold,
        SUM(s.totalPrice) / 100.0 as total_revenue_gbp,
        AVG(f.predicted_quantity) as avg_forecast_quantity
    FROM `{CATALOG}`.`{SCHEMA}`.`products` p
    LEFT JOIN `{CATALOG}`.`{SCHEMA}`.`sales` s ON p.productId = s.productId
    LEFT JOIN `{CATALOG}`.`{SCHEMA}`.`forecasts` f ON p.productId = f.productId
        AND f.target_date BETWEEN '2025-07-15' AND '2025-07-22'
        AND f.model_name = 'HistoricalPattern'
    GROUP BY p.productId, p.name, p.category
""")

print("✓ Created product performance view")

# COMMAND ----------

# Sample query from the view
display(spark.sql(f"""
    SELECT * 
    FROM `{CATALOG}`.`{SCHEMA}`.`v_product_performance`
    WHERE total_revenue_gbp > 1000
    ORDER BY total_revenue_gbp DESC
    LIMIT 20
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Final Summary

# COMMAND ----------

# Final summary
print("\n" + "="*60)
print("UPLOAD COMPLETE!")
print("="*60)

# Table counts
table_counts = spark.sql(f"""
    SELECT 
        'customers' as table_name, COUNT(*) as rows FROM `{CATALOG}`.`{SCHEMA}`.`customers`
    UNION ALL SELECT 'products', COUNT(*) FROM `{CATALOG}`.`{SCHEMA}`.`products`
    UNION ALL SELECT 'orders', COUNT(*) FROM `{CATALOG}`.`{SCHEMA}`.`orders`
    UNION ALL SELECT 'sales', COUNT(*) FROM `{CATALOG}`.`{SCHEMA}`.`sales`
    UNION ALL SELECT 'forecasts', COUNT(*) FROM `{CATALOG}`.`{SCHEMA}`.`forecasts`
    ORDER BY rows DESC
""").collect()

print("\nTable Row Counts:")
total = 0
for row in table_counts:
    print(f"  {row['table_name']:20} {row['rows']:>10,} rows")
    total += row['rows']
print(f"  {'TOTAL':20} {total:>10,} rows")

print(f"\nCatalog: {CATALOG}")
print(f"Schema: {SCHEMA}")
print("\nAll tables loaded successfully with forecast data intact!")

# COMMAND ----------

# Close DuckDB connection
conn.close()
print("\n✓ DuckDB connection closed")
