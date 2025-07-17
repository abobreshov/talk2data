#!/usr/bin/env python3
"""
Verify the final database structure and show sample queries
"""

import duckdb
from pathlib import Path
import json

# Configuration
DATA_DIR = Path(__file__).parent.parent / 'data'
FINAL_DB = DATA_DIR / 'grocery_final.db'

def verify_database():
    """Run verification queries on the final database"""
    print("=" * 60)
    print("Final Database Verification")
    print("=" * 60)
    
    conn = duckdb.connect(str(FINAL_DB), read_only=True)
    
    try:
        # 1. Show all tables and views
        print("\n=== Database Objects ===")
        tables = conn.execute("""
            SELECT table_name, table_type 
            FROM information_schema.tables 
            WHERE table_schema = 'main'
            ORDER BY table_type, table_name
        """).fetchall()
        
        print("\nTables:")
        for name, type_ in tables:
            if type_ == 'BASE TABLE':
                count = conn.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
                print(f"  - {name}: {count:,} records")
        
        print("\nViews:")
        for name, type_ in tables:
            if type_ == 'VIEW':
                print(f"  - {name}")
        
        # 2. Product-SKU relationship example
        print("\n=== Product-SKU Relationship Example ===")
        example = conn.execute("""
            SELECT 
                p.productId,
                p.name,
                p.brandName,
                p.price_gbp,
                COUNT(ps.sku) as num_skus,
                STRING_AGG(ps.sku, ', ') as skus
            FROM products p
            JOIN product_skus ps ON p.productId = ps.productId
            GROUP BY p.productId, p.name, p.brandName, p.price_gbp
            HAVING COUNT(ps.sku) > 1
            ORDER BY num_skus DESC
            LIMIT 5
        """).fetchall()
        
        print("\nProducts with multiple SKUs:")
        for pid, name, brand, price, num_skus, skus in example:
            print(f"\n{name} ({brand})")
            print(f"  ProductId: {pid}")
            print(f"  Price: £{price:.2f}")
            print(f"  Number of SKUs: {num_skus}")
            print(f"  SKUs: {skus[:50]}...")
        
        # 3. Top products by revenue
        print("\n=== Top 5 Products by Revenue ===")
        top_products = conn.execute("""
            SELECT 
                p.productId,
                p.name,
                p.brandName,
                pp.units_sold,
                pp.revenue,
                pp.sales_rank
            FROM product_performance pp
            JOIN products p ON pp.productId = p.productId
            WHERE pp.units_sold > 0
            ORDER BY pp.revenue DESC
            LIMIT 5
        """).fetchall()
        
        for pid, name, brand, units, revenue, rank in top_products:
            print(f"\n{rank}. {name} ({brand})")
            print(f"   ProductId: {pid}")
            print(f"   Units sold: {units:,}")
            print(f"   Revenue: £{revenue:,.2f}")
        
        # 4. Customer segments
        print("\n=== Customer Analytics Summary ===")
        segments = conn.execute("""
            SELECT 
                CASE 
                    WHEN lifetime_value >= 3000 THEN 'VIP'
                    WHEN lifetime_value >= 1500 THEN 'High Value'
                    WHEN lifetime_value >= 500 THEN 'Regular'
                    WHEN lifetime_value > 0 THEN 'Occasional'
                    ELSE 'No Orders'
                END as segment,
                COUNT(*) as customer_count,
                AVG(lifetime_value) as avg_lifetime_value
            FROM customer_analytics
            GROUP BY segment
            ORDER BY avg_lifetime_value DESC
        """).fetchall()
        
        for segment, count, avg_value in segments:
            if avg_value:
                print(f"{segment}: {count} customers (avg: £{avg_value:.2f})")
            else:
                print(f"{segment}: {count} customers")
        
        # 5. Data quality check
        print("\n=== Data Quality Check ===")
        
        # Check for products without SKUs
        no_skus = conn.execute("""
            SELECT COUNT(*) 
            FROM products p
            WHERE NOT EXISTS (
                SELECT 1 FROM product_skus ps 
                WHERE ps.productId = p.productId
            )
        """).fetchone()[0]
        print(f"✓ Products without SKUs: {no_skus}")
        
        # Check for orphaned order items
        orphaned_items = conn.execute("""
            SELECT COUNT(*) 
            FROM order_items oi
            WHERE NOT EXISTS (
                SELECT 1 FROM products p 
                WHERE p.productId = oi.productId
            )
        """).fetchone()[0]
        print(f"✓ Orphaned order items: {orphaned_items}")
        
        # Check price consistency
        price_check = conn.execute("""
            SELECT 
                COUNT(*) as products_checked,
                SUM(CASE WHEN ABS(price_gbp - price_pence/100.0) < 0.01 THEN 1 ELSE 0 END) as correct_prices
            FROM products
        """).fetchone()
        print(f"✓ Price calculation check: {price_check[1]}/{price_check[0]} prices correct")
        
        print("\n" + "=" * 60)
        print("✓ DATABASE VERIFICATION COMPLETE")
        print(f"\nDatabase ready for use: {FINAL_DB}")
        
    except Exception as e:
        print(f"\n✗ Error during verification: {e}")
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    verify_database()