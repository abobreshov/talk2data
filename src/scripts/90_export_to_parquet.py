#!/usr/bin/env python3
"""
Export all tables (not views) from grocery_final.db to Parquet files
For ingestion into Databricks
"""

import duckdb
from pathlib import Path
from datetime import datetime
import json
import shutil

DATA_DIR = Path(__file__).parent.parent / 'data'
DB_PATH = DATA_DIR / 'grocery_final.db'
PARQUET_DIR = DATA_DIR / 'parquet_export'

def export_all_tables_to_parquet():
    """Export all base tables to Parquet format"""
    
    print("=== Exporting All Tables to Parquet ===\n")
    
    try:
        # Create export directory
        if PARQUET_DIR.exists():
            print(f"1. Cleaning existing export directory...")
            shutil.rmtree(PARQUET_DIR)
        
        PARQUET_DIR.mkdir(exist_ok=True)
        print(f"✓ Created export directory: {PARQUET_DIR}\n")
        
        # Connect to database
        conn = duckdb.connect(str(DB_PATH), read_only=True)
        
        # Get all base tables (not views)
        print("2. Getting list of tables...")
        tables = conn.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'main' 
            AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """).fetchall()
        
        table_names = [t[0] for t in tables]
        print(f"Found {len(table_names)} tables to export:")
        for table in table_names:
            print(f"  - {table}")
        
        # Export each table
        print("\n3. Exporting tables to Parquet...")
        export_summary = {
            'export_date': datetime.now().isoformat(),
            'source_database': str(DB_PATH),
            'tables': {}
        }
        
        total_rows = 0
        total_size = 0
        
        for table_name in table_names:
            print(f"\nExporting {table_name}...")
            
            # Get row count
            row_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            
            # Export to Parquet with compression
            parquet_path = PARQUET_DIR / f"{table_name}.parquet"
            conn.execute(f"""
                COPY (SELECT * FROM {table_name}) 
                TO '{parquet_path}' 
                (FORMAT PARQUET, COMPRESSION SNAPPY)
            """)
            
            # Get file size
            file_size = parquet_path.stat().st_size
            
            # Get schema info
            schema_info = conn.execute(f"DESCRIBE {table_name}").fetchall()
            columns = [(col[0], col[1]) for col in schema_info]
            
            # Add to summary
            export_summary['tables'][table_name] = {
                'row_count': row_count,
                'file_size_bytes': file_size,
                'file_size_mb': round(file_size / (1024 * 1024), 2),
                'columns': columns,
                'file_name': f"{table_name}.parquet"
            }
            
            total_rows += row_count
            total_size += file_size
            
            print(f"  ✓ Exported {row_count:,} rows ({file_size / (1024 * 1024):.2f} MB)")
        
        # Add totals to summary
        export_summary['summary'] = {
            'total_tables': len(table_names),
            'total_rows': total_rows,
            'total_size_bytes': total_size,
            'total_size_mb': round(total_size / (1024 * 1024), 2)
        }
        
        # Save export summary
        summary_path = PARQUET_DIR / 'export_summary.json'
        with open(summary_path, 'w') as f:
            json.dump(export_summary, f, indent=2)
        
        print("\n4. Export Summary:")
        print(f"  Total tables: {len(table_names)}")
        print(f"  Total rows: {total_rows:,}")
        print(f"  Total size: {total_size / (1024 * 1024):.2f} MB")
        
        # Create upload instructions
        print("\n5. Creating Databricks upload guide...")
        
        upload_guide = f"""# Databricks Upload Guide

## Export Information
- Export Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
- Total Tables: {len(table_names)}
- Total Size: {total_size / (1024 * 1024):.2f} MB

## Files to Upload

All files are located in: `{PARQUET_DIR}`

### Tables ({len(table_names)} files)
"""
        
        for table_name in sorted(table_names):
            info = export_summary['tables'][table_name]
            upload_guide += f"- `{table_name}.parquet` ({info['file_size_mb']} MB) - {info['row_count']:,} rows\n"
        
        upload_guide += f"""
### Metadata
- `export_summary.json` - Export metadata and statistics

## Upload Steps

### Option 1: Using Databricks CLI
```bash
# Upload all parquet files
databricks fs cp -r {PARQUET_DIR}/*.parquet dbfs:/FileStore/grocery_poc/

# Upload summary
databricks fs cp {PARQUET_DIR}/export_summary.json dbfs:/FileStore/grocery_poc/
```

### Option 2: Using Databricks UI
1. Go to Data → Add Data → Upload File
2. Select all .parquet files from {PARQUET_DIR}
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
with open(f"{{base_path}}/export_summary.json", "r") as f:
    export_info = json.load(f)

# Create Delta tables from Parquet files
for table_name in export_info['tables'].keys():
    print(f"Loading {table_name}...")
    
    # Read Parquet
    df = spark.read.parquet(f"{{base_path}}/{{table_name}}.parquet")
    
    # Write as Delta table
    df.write.mode("overwrite").saveAsTable(f"grocery_poc.{{table_name}}")
    
    # Show count
    count = spark.sql(f"SELECT COUNT(*) FROM grocery_poc.{{table_name}}").collect()[0][0]
    print(f"✓ Created {{table_name}} with {{count:,}} rows")
```

## Verify Data

```python
# Check all tables
tables = spark.sql("SHOW TABLES IN grocery_poc").collect()
print(f"\\nTotal tables: {len(tables)}")
for table in tables:
    print(f"  - {{table.tableName}}")

# Sample queries
print("\\n=== Sample Data ===")
spark.sql("SELECT * FROM grocery_poc.products LIMIT 5").show()
spark.sql("SELECT COUNT(*) as order_count FROM grocery_poc.orders").show()
```
"""
        
        guide_path = PARQUET_DIR / 'DATABRICKS_UPLOAD_GUIDE.md'
        with open(guide_path, 'w') as f:
            f.write(upload_guide)
        
        print(f"✓ Created upload guide: {guide_path}")
        
        # Close connection
        conn.close()
        
        print(f"\n✓ Export completed successfully!")
        print(f"\nAll files exported to: {PARQUET_DIR}")
        print(f"Total size: {total_size / (1024 * 1024):.2f} MB")
        print(f"\nReady for Databricks upload!")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        raise

if __name__ == "__main__":
    export_all_tables_to_parquet()