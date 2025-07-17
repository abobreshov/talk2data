# Type Conversion Solution for Databricks Upload

## Problem Summary

When uploading DuckDB tables to Databricks, tables with special data types were failing with various conversion errors:
- "Failed to merge incompatible data types DecimalType(10,2) and DoubleType" 
- "Failed to merge fields 'expiration_date' and 'expiration_date'" (stock table with DATE columns)
- "Failed to merge fields 'predicted_quantity' and 'predicted_quantity'" (forecasts table with FLOAT columns)
- "Unsupported arrow type Time(MICROSECOND, 64)" (supplier_schedules table with TIME columns)
- Arrow optimization errors when using Python Decimal objects
- DBFS access restrictions preventing temporary file usage

## Solution

The final working solution handles each problematic data type:

```python
# Handle TIME columns before creating Spark DataFrame
time_columns = [col_name for col_name, data_type in columns_info if data_type.upper() == 'TIME']
has_time_columns = len(time_columns) > 0

if has_time_columns:
    # Convert TIME columns to strings (HH:MM:SS format)
    for col in time_columns:
        if col in df_pandas.columns:
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
```

## Why This Works

1. **Simple and Direct**: Uses native Spark DataFrame operations with `withColumn` and `cast`
2. **Handles TIME columns**: Converts TIME to strings AND uses explicit schema to bypass Arrow serialization issues
3. **FLOAT to DOUBLE conversion**: Prevents "Failed to merge incompatible data types DoubleType and FloatType" errors
4. **Preserves Precision**: Maintains exact DECIMAL precision and scale from DuckDB
5. **Universal Compatibility**: Works with all Databricks runtime versions
6. **Explicit Schema for TIME tables**: Creating the schema manually prevents Arrow from attempting to serialize TIME types

## Tables Affected

Tables with special type columns that benefit from this solution:

**DECIMAL columns:**
- `products` - price column (DECIMAL(10,2))
- `orders` - totalAmount, discountAmount columns (DECIMAL)
- `order_items` - price, totalPrice columns (DECIMAL)
- `sales` - price, totalPrice columns (DECIMAL)
- `stock` - purchase_price column (DECIMAL(10,2))
- `purchase_orders` - total_cost column (DECIMAL)
- `inbound_deliveries` - cost column (DECIMAL)

**DATE columns:**
- `stock` - expiration_date, received_date columns
- `forecasts` - forecast_date, target_date columns
- `orders` - orderDate, deliveryDate columns
- `supplier_schedules` - order_day_of_week column
- `product_ordering_calendar` - cal_date column
- `purchase_orders` - order_date, expected_delivery_date columns
- `inbound_deliveries` - delivery_date, processed_date columns

**TIMESTAMP columns:**
- `stock` - last_updated column
- `forecasts` - created_at column
- `orders` - createdAt column
- `live_basket` - expiration_time column

**FLOAT columns:**
- `forecasts` - predicted_quantity, confidence_lower, confidence_upper columns

**TIME columns:**
- `supplier_schedules` - po_cutoff_time column

## Implementation Details

The solution is implemented in the `direct_duckdb_upload_notebook.py` file, specifically in the table loading loop (lines 226-270). The key steps are:

1. **Detect DECIMAL columns** from DuckDB schema information
2. **Convert to float** in pandas DataFrame for initial compatibility
3. **Create temporary view** in Spark
4. **Build SQL query** with explicit CAST operations
5. **Execute query** to get properly typed DataFrame
6. **Clean up** temporary view

This approach ensures all DECIMAL columns maintain their exact precision and scale when loaded into Databricks Unity Catalog.