#!/usr/bin/env python3
"""
Create product-SKU mapping table.
Generates multiple SKUs per product for warehouse management.
"""

import duckdb
import pandas as pd
import numpy as np
from pathlib import Path
import random
import sys

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

DATA_DIR = Path(__file__).parent.parent / 'data'

def create_product_sku_mapping():
    """Create product_skus table with multiple SKUs per product"""
    
    print("=== Creating Product-SKU Mapping ===\n")
    
    db_path = DATA_DIR / 'products.duckdb'
    
    # Check if database exists
    if not db_path.exists():
        print(f"❌ Error: Products database not found at {db_path}")
        print("Please run 12_load_products.py first.")
        return False
    
    # Connect to database
    conn = duckdb.connect(str(db_path))
    
    # Get all products
    print("1. Loading products...")
    products = conn.execute("""
        SELECT productId, name, category 
        FROM products 
        ORDER BY productId
    """).fetchdf()
    print(f"   Found {len(products):,} products")
    
    # Create SKU mapping table
    print("\n2. Creating product_skus table...")
    conn.execute("""
        CREATE OR REPLACE TABLE product_skus (
            productId VARCHAR NOT NULL,
            sku VARCHAR PRIMARY KEY,
            sku_suffix VARCHAR NOT NULL,
            is_primary BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (productId) REFERENCES products(productId)
        )
    """)
    
    # Generate SKUs
    print("\n3. Generating SKUs...")
    sku_data = []
    
    # SKU distribution: ~1/3 each of 1, 2, or 3 SKUs per product
    sku_distribution = [1, 1, 1, 2, 2, 2, 3, 3, 3]
    
    for _, product in products.iterrows():
        product_id = product['productId']
        num_skus = random.choice(sku_distribution)
        
        for i in range(num_skus):
            sku_suffix = f"{i+1:04d}"  # 0001, 0002, 0003
            sku = f"{product_id}{sku_suffix}"
            
            sku_data.append({
                'productId': product_id,
                'sku': sku,
                'sku_suffix': sku_suffix,
                'is_primary': i == 0  # First SKU is primary
            })
    
    # Insert SKUs
    print(f"   Generated {len(sku_data):,} SKUs")
    sku_df = pd.DataFrame(sku_data)
    conn.execute("INSERT INTO product_skus SELECT * FROM sku_df")
    
    # Verify and show statistics
    print("\n4. SKU Distribution:")
    stats = conn.execute("""
        SELECT 
            sku_count,
            COUNT(*) as products,
            ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) as percentage
        FROM (
            SELECT productId, COUNT(*) as sku_count
            FROM product_skus
            GROUP BY productId
        )
        GROUP BY sku_count
        ORDER BY sku_count
    """).fetchall()
    
    for sku_count, products, pct in stats:
        print(f"   - {sku_count} SKU(s): {products:,} products ({pct}%)")
    
    # Show sample mappings
    print("\n5. Sample SKU Mappings:")
    samples = conn.execute("""
        SELECT 
            p.productId,
            p.name,
            COUNT(ps.sku) as num_skus,
            STRING_AGG(ps.sku_suffix, ', ') as sku_suffixes
        FROM products p
        JOIN product_skus ps ON p.productId = ps.productId
        GROUP BY p.productId, p.name
        ORDER BY RANDOM()
        LIMIT 5
    """).fetchall()
    
    for pid, name, num_skus, suffixes in samples:
        print(f"   - {pid}: {name[:30]:<30} | {num_skus} SKUs: {suffixes}")
    
    # Summary
    total_skus = conn.execute("SELECT COUNT(*) FROM product_skus").fetchone()[0]
    avg_skus = conn.execute("""
        SELECT AVG(sku_count) FROM (
            SELECT COUNT(*) as sku_count 
            FROM product_skus 
            GROUP BY productId
        )
    """).fetchone()[0]
    
    print(f"\n6. Summary:")
    print(f"   - Total SKUs: {total_skus:,}")
    print(f"   - Average SKUs per product: {avg_skus:.2f}")
    
    conn.close()
    print(f"\n✓ Product-SKU mapping created successfully")
    return True

if __name__ == "__main__":
    create_product_sku_mapping()