#!/usr/bin/env python3
"""
Check database schemas to understand the structure
"""

import duckdb
from pathlib import Path

DATA_DIR = Path(__file__).parent.parent / 'data'

print("=== Checking Database Schemas ===\n")

# Check grocery_final.db
print("1. grocery_final.db - products table:")
try:
    conn = duckdb.connect(str(DATA_DIR / 'grocery_final.db'), read_only=True)
    schema = conn.execute("DESCRIBE products").df()
    print(schema[['column_name', 'column_type']])
    conn.close()
except Exception as e:
    print(f"Error: {e}")

print("\n2. Trying to check products.duckdb:")
try:
    # Try read-only mode to avoid lock issues
    conn = duckdb.connect(str(DATA_DIR / 'products.duckdb'), read_only=True)
    schema = conn.execute("DESCRIBE products").df()
    print(schema[['column_name', 'column_type']])
    
    # Check if we have any sample data
    sample = conn.execute("SELECT * FROM products LIMIT 1").df()
    print("\nSample data columns:")
    print(sample.columns.tolist())
    conn.close()
except Exception as e:
    print(f"Error: {e}")
    print("\nTrying alternative: check parquet export")
    
    # Check parquet file as fallback
    try:
        import pandas as pd
        df = pd.read_parquet(DATA_DIR / 'parquet' / 'products.parquet')
        print("\nColumns in products.parquet:")
        print(df.columns.tolist())
        print("\nFirst row to see data:")
        if len(df) > 0:
            print(df.iloc[0])
    except Exception as e2:
        print(f"Parquet error: {e2}")

print("\n3. Checking original scraped data:")
try:
    # Check the original CSV from data extraction
    csv_path = DATA_DIR.parent / 'data-extraction' / 'data' / 'products.csv'
    if csv_path.exists():
        import pandas as pd
        df = pd.read_csv(csv_path, nrows=5)
        print(f"\nColumns in {csv_path}:")
        print(df.columns.tolist())
        print("\nFirst row:")
        print(df.iloc[0])
    else:
        print(f"CSV not found at {csv_path}")
except Exception as e:
    print(f"CSV error: {e}")