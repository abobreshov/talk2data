#!/usr/bin/env python3
"""
Load generated CSV data into the orders database
"""

import os
import sys
import pandas as pd
import duckdb
from pathlib import Path
from datetime import datetime

# Add scripts directory to path for imports
sys.path.append(str(Path(__file__).parent))

# Import configuration
from utilities.config import DATA_DIR, PRODUCTS_DB, ORDERS_DB, CUSTOMERS_CSV, get_config

# Get config for compatibility
config = get_config()

def load_data_to_database():
    """Load all CSV data into the orders database"""
    print("=" * 60)
    print("Loading Data to Orders Database")
    print("=" * 60)
    print(f"Execution time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Connect to database
    print("\n=== Connecting to Orders Database ===")
    conn = duckdb.connect(str(ORDERS_DB))
    
    try:
        # Drop existing tables to ensure clean load
        print("\n=== Dropping Existing Tables ===")
        for table in ['sales', 'order_items', 'orders']:
            try:
                conn.execute(f"DROP TABLE IF EXISTS {table}")
                print(f"✓ Dropped table {table}")
            except:
                pass
        
        # Load orders
        print("\n=== Loading Orders ===")
        orders_csv = DATA_DIR / 'orders.csv'
        conn.execute(f"""
            CREATE TABLE orders AS 
            SELECT * FROM read_csv_auto('{orders_csv}')
        """)
        order_count = conn.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
        print(f"✓ Loaded {order_count:,} orders")
        
        # Load order items
        print("\n=== Loading Order Items ===")
        order_items_csv = DATA_DIR / 'order_items.csv'
        conn.execute(f"""
            CREATE TABLE order_items AS 
            SELECT * FROM read_csv_auto('{order_items_csv}')
        """)
        item_count = conn.execute("SELECT COUNT(*) FROM order_items").fetchone()[0]
        print(f"✓ Loaded {item_count:,} order items")
        
        # Load sales
        print("\n=== Loading Sales ===")
        sales_csv = DATA_DIR / 'sales.csv'
        conn.execute(f"""
            CREATE TABLE sales AS 
            SELECT * FROM read_csv_auto('{sales_csv}')
        """)
        sales_count = conn.execute("SELECT COUNT(*) FROM sales").fetchone()[0]
        print(f"✓ Loaded {sales_count:,} sales records")
        
        # Create indexes for better performance
        print("\n=== Creating Indexes ===")
        
        # Orders indexes
        conn.execute("CREATE INDEX idx_orders_customer ON orders(customerId)")
        conn.execute("CREATE INDEX idx_orders_status ON orders(orderStatus)")
        conn.execute("CREATE INDEX idx_orders_date ON orders(orderDate)")
        print("✓ Created indexes on orders table")
        
        # Order items indexes
        conn.execute("CREATE INDEX idx_items_order ON order_items(orderId)")
        conn.execute("CREATE INDEX idx_items_product ON order_items(productId)")
        print("✓ Created indexes on order_items table")
        
        # Sales indexes
        conn.execute("CREATE INDEX idx_sales_order ON sales(orderId)")
        conn.execute("CREATE INDEX idx_sales_product ON sales(productId)")
        conn.execute("CREATE INDEX idx_sales_customer ON sales(customerId)")
        conn.execute("CREATE INDEX idx_sales_date ON sales(saleDate)")
        print("✓ Created indexes on sales table")
        
        # Verify data
        print("\n=== Verifying Data ===")
        tables = ['orders', 'order_items', 'sales']
        for table in tables:
            count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            print(f"✓ Table {table}: {count:,} records")
        
        # Show sample queries
        print("\n=== Sample Queries ===")
        
        # Total revenue
        total_revenue = conn.execute("""
            SELECT SUM(totalAmount) as revenue 
            FROM orders 
            WHERE orderStatus = 'DELIVERED'
        """).fetchone()[0]
        print(f"Total revenue from delivered orders: £{total_revenue:,.2f}")
        
        # Top products
        print("\nTop 5 products by sales:")
        top_products = conn.execute("""
            SELECT productId, COUNT(*) as units_sold, SUM(unitPrice) as revenue
            FROM sales
            GROUP BY productId
            ORDER BY units_sold DESC
            LIMIT 5
        """).fetchall()
        
        for i, (product_id, units, revenue) in enumerate(top_products, 1):
            print(f"  {i}. Product {product_id}: {units:,} units, £{revenue:,.2f}")
        
        print("\n" + "=" * 60)
        print("✓ DATABASE LOAD COMPLETE")
        print("All data successfully loaded into orders.db")
        
    except Exception as e:
        print(f"\n✗ Error loading data: {e}")
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    load_data_to_database()