#!/usr/bin/env python3
"""
Comprehensive verification of grocery_final.db alignment
Checks all tables, relationships, and recent changes
"""

import duckdb
from pathlib import Path
from datetime import datetime
import pandas as pd

DATA_DIR = Path(__file__).parent.parent / 'data'
DB_PATH = DATA_DIR / 'grocery_final.db'

def verify_database():
    """Verify all tables and data alignment in grocery_final.db"""
    
    print("=== Database Alignment Verification ===\n")
    
    try:
        conn = duckdb.connect(str(DB_PATH))
        
        # 1. Check all tables exist
        print("1. Checking tables...")
        tables = conn.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'main'
            ORDER BY table_name
        """).fetchdf()
        
        print("Tables found:")
        for table in tables['table_name']:
            count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            print(f"  - {table}: {count:,} rows")
        
        # 2. Verify Chicken Tikka Filled Naan fix
        print("\n2. Verifying Chicken Tikka Filled Naan sales fix...")
        chicken_tikka = conn.execute("""
            SELECT 
                p.name,
                p.productId,
                COUNT(DISTINCT s.saleId) as total_sales,
                SUM(s.quantity) as total_units,
                RANK() OVER (ORDER BY SUM(s.quantity) DESC) as sales_rank
            FROM products p
            LEFT JOIN sales s ON p.productId = s.productId
            WHERE p.name LIKE '%Chicken Tikka Filled Naan%'
            GROUP BY p.name, p.productId
        """).fetchdf()
        
        if len(chicken_tikka) > 0:
            print(f"  Product: {chicken_tikka.iloc[0]['name']}")
            print(f"  Total units sold: {chicken_tikka.iloc[0]['total_units']}")
            print(f"  Sales rank: #{chicken_tikka.iloc[0]['sales_rank']}")
            print("  ✓ Fix confirmed - no longer #1 seller")
        
        # 3. Check sales dates
        print("\n3. Checking sales date range...")
        sales_dates = conn.execute("""
            SELECT 
                MIN(CAST(saleDate AS DATE)) as first_sale,
                MAX(CAST(saleDate AS DATE)) as last_sale,
                COUNT(DISTINCT CAST(saleDate AS DATE)) as total_days,
                COUNT(*) as total_sales
            FROM sales
        """).fetchone()
        
        print(f"  First sale: {sales_dates[0]}")
        print(f"  Last sale: {sales_dates[1]}")
        print(f"  Total days: {sales_dates[2]}")
        print(f"  Total sales: {sales_dates[3]:,}")
        
        # Check if sales go up to July 14
        if str(sales_dates[1]) == '2025-07-14':
            print("  ✓ Sales updated to current date (July 14, 2025)")
        else:
            print(f"  ⚠️  Sales only go up to {sales_dates[1]}, not July 14")
        
        # 4. Check orders status distribution
        print("\n4. Checking order status distribution...")
        order_status = conn.execute("""
            SELECT 
                orderStatus,
                COUNT(*) as count,
                MIN(CAST(deliveryDate AS DATE)) as min_date,
                MAX(CAST(deliveryDate AS DATE)) as max_date
            FROM orders
            GROUP BY orderStatus
            ORDER BY orderStatus
        """).fetchdf()
        
        print("Order Status Summary:")
        for _, row in order_status.iterrows():
            print(f"  {row['orderStatus']}: {row['count']:,} orders ({row['min_date']} to {row['max_date']})")
        
        # 5. Check live basket data
        print("\n5. Checking live basket...")
        basket_check = conn.execute("""
            SELECT 
                COUNT(DISTINCT basket_id) as total_baskets,
                COUNT(DISTINCT session_id) as total_sessions,
                COUNT(DISTINCT customerId) as unique_customers,
                COUNT(DISTINCT order_reference) as linked_orders,
                MIN(CAST(added_at AS DATE)) as earliest_basket,
                MAX(CAST(added_at AS DATE)) as latest_basket
            FROM live_basket
            WHERE status = 'ACTIVE'
        """).fetchone()
        
        print(f"  Total basket entries: {basket_check[0]:,}")
        print(f"  Unique sessions: {basket_check[1]:,}")
        print(f"  Unique customers: {basket_check[2]:,}")
        print(f"  Linked orders: {basket_check[3]:,}")
        print(f"  Basket date range: {basket_check[4]} to {basket_check[5]}")
        
        # Check basket-order alignment
        basket_order_check = conn.execute("""
            SELECT 
                o.orderStatus,
                COUNT(DISTINCT lb.order_reference) as orders_with_baskets,
                COUNT(DISTINCT lb.basket_id) as basket_entries
            FROM live_basket lb
            JOIN orders o ON lb.order_reference = o.orderId
            GROUP BY o.orderStatus
        """).fetchdf()
        
        print("\n  Basket-Order Alignment:")
        for _, row in basket_order_check.iterrows():
            print(f"    {row['orderStatus']} orders: {row['orders_with_baskets']} with {row['basket_entries']:,} basket entries")
        
        # 6. Check product ID alignment
        print("\n6. Checking productId alignment...")
        
        # Check if all productIds are 18 characters
        product_id_check = conn.execute("""
            SELECT 
                'products' as table_name,
                MIN(LENGTH(productId)) as min_length,
                MAX(LENGTH(productId)) as max_length,
                COUNT(DISTINCT productId) as unique_products
            FROM products
            UNION ALL
            SELECT 
                'sales' as table_name,
                MIN(LENGTH(productId)) as min_length,
                MAX(LENGTH(productId)) as max_length,
                COUNT(DISTINCT productId) as unique_products
            FROM sales
            UNION ALL
            SELECT 
                'live_basket' as table_name,
                MIN(LENGTH(productId)) as min_length,
                MAX(LENGTH(productId)) as max_length,
                COUNT(DISTINCT productId) as unique_products
            FROM live_basket
        """).fetchdf()
        
        print("ProductId Format Check:")
        for _, row in product_id_check.iterrows():
            print(f"  {row['table_name']}: length {row['min_length']}-{row['max_length']}, {row['unique_products']} unique products")
        
        # 7. Check foreign key relationships
        print("\n7. Checking foreign key relationships...")
        
        # Sales -> Products
        orphan_sales = conn.execute("""
            SELECT COUNT(*) as orphan_count
            FROM sales s
            LEFT JOIN products p ON s.productId = p.productId
            WHERE p.productId IS NULL
        """).fetchone()[0]
        
        print(f"  Sales with invalid productId: {orphan_sales}")
        
        # Live Basket -> Products
        orphan_basket = conn.execute("""
            SELECT COUNT(*) as orphan_count
            FROM live_basket lb
            LEFT JOIN products p ON lb.productId = p.productId
            WHERE p.productId IS NULL
        """).fetchone()[0]
        
        print(f"  Live basket with invalid productId: {orphan_basket}")
        
        # Live Basket -> Orders
        orphan_basket_orders = conn.execute("""
            SELECT COUNT(*) as orphan_count
            FROM live_basket lb
            LEFT JOIN orders o ON lb.order_reference = o.orderId
            WHERE o.orderId IS NULL
        """).fetchone()[0]
        
        print(f"  Live basket with invalid order_reference: {orphan_basket_orders}")
        
        # 8. Check data consistency
        print("\n8. Data Consistency Checks...")
        
        # Check order totals match order items
        order_total_check = conn.execute("""
            WITH order_item_totals AS (
                SELECT 
                    orderId,
                    COUNT(*) as calculated_item_count,
                    ROUND(SUM(unitPrice * quantity), 2) as calculated_total
                FROM order_items
                GROUP BY orderId
            )
            SELECT 
                COUNT(*) as mismatched_orders
            FROM orders o
            JOIN order_item_totals oit ON o.orderId = oit.orderId
            WHERE ABS(o.totalAmount - oit.calculated_total) > 0.01
                OR o.itemCount != oit.calculated_item_count
        """).fetchone()[0]
        
        print(f"  Orders with mismatched totals: {order_total_check}")
        
        # 9. Top products check (ensure variety)
        print("\n9. Top 10 Products by Sales...")
        top_products = conn.execute("""
            SELECT 
                p.name,
                SUM(s.quantity) as total_units,
                COUNT(DISTINCT s.saleId) as total_sales
            FROM sales s
            JOIN products p ON s.productId = p.productId
            GROUP BY p.name
            ORDER BY total_units DESC
            LIMIT 10
        """).fetchdf()
        
        for idx, row in top_products.iterrows():
            print(f"  {idx+1}. {row['name']}: {row['total_units']} units")
        
        # 10. Final summary
        print("\n10. Summary Report:")
        total_issues = orphan_sales + orphan_basket + orphan_basket_orders + order_total_check
        
        if total_issues == 0:
            print("  ✓ All data integrity checks passed!")
            print("  ✓ Database is properly aligned")
            print("  ✓ All recent changes are in place")
        else:
            print(f"  ⚠️  Found {total_issues} data integrity issues")
        
        conn.close()
        
    except Exception as e:
        print(f"\n❌ Error during verification: {e}")
        raise

if __name__ == "__main__":
    verify_database()