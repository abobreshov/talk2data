#!/usr/bin/env python3
"""
Load products from CSV into DuckDB database.
Creates the products table from scraped data.
"""

import duckdb
import pandas as pd
from pathlib import Path
import sys

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

DATA_DIR = Path(__file__).parent.parent / 'data'
EXTRACTION_DATA_DIR = Path(__file__).parent.parent / 'data-extraction' / 'data'

def load_products_to_database():
    """Load products from CSV and create products table"""
    
    print("=== Loading Products to Database ===\n")
    
    # Input and output paths
    csv_path = EXTRACTION_DATA_DIR / 'products.csv'
    db_path = DATA_DIR / 'products.duckdb'
    
    # Check if CSV exists
    if not csv_path.exists():
        print(f"❌ Error: Products CSV not found at {csv_path}")
        print("Please run the data extraction scripts first.")
        return False
    
    # Load CSV
    print(f"1. Loading products from {csv_path}")
    products_df = pd.read_csv(csv_path)
    print(f"   Loaded {len(products_df):,} products")
    
    # Connect to database
    print(f"\n2. Creating database at {db_path}")
    conn = duckdb.connect(str(db_path))
    
    # Create products table with proper schema
    print("\n3. Creating products table...")
    conn.execute("""
        CREATE OR REPLACE TABLE products (
            productId VARCHAR PRIMARY KEY,
            sku VARCHAR,
            name VARCHAR NOT NULL,
            brandName VARCHAR,
            sellingSize VARCHAR,
            category VARCHAR,
            subcategory VARCHAR,
            currency VARCHAR DEFAULT 'GBP',
            price_pence INTEGER,
            price_gbp DECIMAL(10, 2) GENERATED ALWAYS AS (price_pence / 100.0) VIRTUAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Insert data
    print("\n4. Inserting product data...")
    conn.execute("INSERT INTO products SELECT * FROM products_df")
    
    # Verify
    count = conn.execute("SELECT COUNT(*) FROM products").fetchone()[0]
    print(f"   ✓ Inserted {count:,} products")
    
    # Show sample
    print("\n5. Sample products:")
    sample = conn.execute("""
        SELECT productId, name, category, price_gbp 
        FROM products 
        LIMIT 5
    """).fetchall()
    
    for pid, name, cat, price in sample:
        print(f"   - {pid}: {name[:40]:<40} | {cat:<20} | £{price:.2f}")
    
    # Category summary
    print("\n6. Category Summary:")
    categories = conn.execute("""
        SELECT category, COUNT(*) as count 
        FROM products 
        GROUP BY category 
        ORDER BY count DESC
    """).fetchall()
    
    for cat, count in categories:
        print(f"   - {cat}: {count:,} products")
    
    conn.close()
    print(f"\n✓ Products database created at {db_path}")
    return True

if __name__ == "__main__":
    load_products_to_database()