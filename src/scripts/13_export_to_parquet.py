#!/usr/bin/env python3
"""
Export final DuckDB database to Parquet format
This script exports all tables from the final DuckDB database to Parquet files
for easy integration with other data processing tools.
"""

import os
import sys
import duckdb
from pathlib import Path
from datetime import datetime
import json

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

# Load configuration
config_path = Path(__file__).parent / "config.json"
with open(config_path, 'r') as f:
    config = json.load(f)

DATA_DIR = Path(config['data_dir'])
FINAL_DB = DATA_DIR / 'grocery_final.db'
PARQUET_DIR = DATA_DIR / 'parquet'

def export_to_parquet():
    """Export all tables from DuckDB to Parquet format"""
    print("=" * 60)
    print("Exporting DuckDB to Parquet Format")
    print("=" * 60)
    print(f"Execution time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Check if database exists
    if not FINAL_DB.exists():
        print(f"✗ Error: Database not found at {FINAL_DB}")
        print("  Please run 11_create_final_database.py first")
        return False
    
    # Create parquet directory
    PARQUET_DIR.mkdir(parents=True, exist_ok=True)
    print(f"\n✓ Created parquet directory: {PARQUET_DIR}")
    
    # Connect to database
    print(f"\n=== Connecting to Database ===")
    conn = duckdb.connect(str(FINAL_DB), read_only=True)
    
    try:
        # Get list of tables
        tables = conn.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'main' 
            AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """).fetchall()
        
        print(f"\n✓ Found {len(tables)} tables to export")
        
        # Export each table
        print(f"\n=== Exporting Tables ===")
        export_summary = {}
        
        for (table_name,) in tables:
            print(f"\nExporting {table_name}...")
            
            # Get row count
            row_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            
            # Export to parquet
            parquet_path = PARQUET_DIR / f"{table_name}.parquet"
            conn.execute(f"""
                COPY {table_name} 
                TO '{parquet_path}' 
                (FORMAT PARQUET, COMPRESSION 'SNAPPY')
            """)
            
            # Verify file size
            file_size_mb = parquet_path.stat().st_size / (1024 * 1024)
            
            print(f"  ✓ Exported {row_count:,} rows")
            print(f"  ✓ File size: {file_size_mb:.2f} MB")
            print(f"  ✓ Path: {parquet_path}")
            
            export_summary[table_name] = {
                'rows': row_count,
                'file_size_mb': round(file_size_mb, 2),
                'file_path': str(parquet_path)
            }
        
        # Export views as well (materialized)
        print(f"\n=== Exporting Views (Materialized) ===")
        
        views = conn.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'main' 
            AND table_type = 'VIEW'
            ORDER BY table_name
        """).fetchall()
        
        for (view_name,) in views:
            print(f"\nExporting view {view_name}...")
            
            # Create materialized version
            parquet_path = PARQUET_DIR / f"{view_name}_view.parquet"
            conn.execute(f"""
                COPY (SELECT * FROM {view_name}) 
                TO '{parquet_path}' 
                (FORMAT PARQUET, COMPRESSION 'SNAPPY')
            """)
            
            # Get row count
            row_count = conn.execute(f"SELECT COUNT(*) FROM {view_name}").fetchone()[0]
            file_size_mb = parquet_path.stat().st_size / (1024 * 1024)
            
            print(f"  ✓ Exported {row_count:,} rows")
            print(f"  ✓ File size: {file_size_mb:.2f} MB")
            
            export_summary[f"{view_name}_view"] = {
                'rows': row_count,
                'file_size_mb': round(file_size_mb, 2),
                'file_path': str(parquet_path),
                'type': 'view'
            }
        
        # Save export summary
        summary_data = {
            'export_timestamp': datetime.now().isoformat(),
            'source_database': str(FINAL_DB),
            'parquet_directory': str(PARQUET_DIR),
            'tables_exported': len(tables),
            'views_exported': len(views),
            'total_files': len(tables) + len(views),
            'exports': export_summary
        }
        
        with open(PARQUET_DIR / 'export_summary.json', 'w') as f:
            json.dump(summary_data, f, indent=2)
        
        print("\n" + "=" * 60)
        print("✓ PARQUET EXPORT COMPLETE")
        print(f"\nSummary:")
        print(f"  - Tables exported: {len(tables)}")
        print(f"  - Views exported: {len(views)}")
        print(f"  - Output directory: {PARQUET_DIR}")
        
        # Calculate total size
        total_size_mb = sum(info['file_size_mb'] for info in export_summary.values())
        print(f"  - Total size: {total_size_mb:.2f} MB")
        
        print("\n✓ Export summary saved to: export_summary.json")
        
        # Show sample queries
        print("\n=== Sample Usage ===")
        print("To read parquet files in Python:")
        print("  import pandas as pd")
        print("  import pyarrow.parquet as pq")
        print(f"  df = pd.read_parquet('{PARQUET_DIR}/products.parquet')")
        print("\nOr with DuckDB:")
        print("  import duckdb")
        print(f"  conn.execute(\"SELECT * FROM read_parquet('{PARQUET_DIR}/products.parquet')\")")
        
        return True
        
    except Exception as e:
        print(f"\n✗ Error during export: {e}")
        return False
    finally:
        conn.close()

def verify_parquet_files():
    """Verify the exported Parquet files"""
    print("\n" + "=" * 60)
    print("Verifying Parquet Files")
    print("=" * 60)
    
    if not PARQUET_DIR.exists():
        print("✗ Parquet directory not found")
        return False
    
    # Create temporary connection for verification
    conn = duckdb.connect(':memory:')
    
    try:
        parquet_files = list(PARQUET_DIR.glob('*.parquet'))
        print(f"\nFound {len(parquet_files)} parquet files")
        
        # Verify each file
        for parquet_file in sorted(parquet_files):
            # Read parquet file
            df = conn.execute(f"""
                SELECT COUNT(*) as row_count, 
                       COUNT(*) FILTER (WHERE 1=1) as non_null_count
                FROM read_parquet('{parquet_file}')
            """).fetchone()
            
            print(f"\n✓ {parquet_file.name}")
            print(f"  - Rows: {df[0]:,}")
            
            # Show schema for first file as example
            if parquet_file.name == 'products.parquet':
                print("  - Sample schema:")
                # Use DESCRIBE to get schema
                schema = conn.execute(f"""
                    DESCRIBE SELECT * FROM read_parquet('{parquet_file}')
                """).df()
                for _, row in schema.head(5).iterrows():
                    print(f"    • {row['column_name']}: {row['column_type']}")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"\n✗ Error during verification: {e}")
        conn.close()
        return False

def main():
    """Main function"""
    # Export to parquet
    success = export_to_parquet()
    
    if success:
        # Verify exports
        verify_parquet_files()
    else:
        sys.exit(1)

if __name__ == "__main__":
    main()