#!/usr/bin/env python3
"""
Comprehensive validation of grocery_final.db schema and data integrity
"""

import duckdb
from pathlib import Path
import pandas as pd
from colorama import init, Fore, Style

init()

def print_header(text):
    print(f"\n{Fore.CYAN}{'='*60}{Style.RESET_ALL}")
    print(f"{Fore.CYAN}{text}{Style.RESET_ALL}")
    print(f"{Fore.CYAN}{'='*60}{Style.RESET_ALL}")

def print_success(text):
    print(f"{Fore.GREEN}✓ {text}{Style.RESET_ALL}")

def print_error(text):
    print(f"{Fore.RED}✗ {text}{Style.RESET_ALL}")

def print_warning(text):
    print(f"{Fore.YELLOW}⚠ {text}{Style.RESET_ALL}")

def validate_schema(conn):
    """Validate database schema"""
    print_header("Schema Validation")
    
    # Expected tables
    expected_tables = ['products', 'product_skus', 'customers', 'orders', 'order_items', 'sales']
    
    # Get actual tables
    actual_tables = conn.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'main' 
        AND table_type = 'BASE TABLE'
    """).df()['table_name'].tolist()
    
    print("\nChecking tables:")
    for table in expected_tables:
        if table in actual_tables:
            print_success(f"Table '{table}' exists")
        else:
            print_error(f"Table '{table}' is missing")
    
    # Check for unexpected tables
    extra_tables = set(actual_tables) - set(expected_tables)
    if extra_tables:
        print_warning(f"Extra tables found: {extra_tables}")
    
    return len(set(expected_tables) - set(actual_tables)) == 0

def check_primary_keys(conn):
    """Check primary key constraints"""
    print_header("Primary Key Validation")
    
    # Check for primary keys and duplicates
    pk_checks = [
        ('products', 'productId'),
        ('product_skus', 'sku'),
        ('customers', 'customerId'),
        ('orders', 'orderId'),
        ('order_items', 'orderItemId'),
        ('sales', 'saleId')
    ]
    
    all_valid = True
    
    for table, pk_column in pk_checks:
        try:
            # Check for duplicates
            result = conn.execute(f"""
                SELECT {pk_column}, COUNT(*) as cnt
                FROM {table}
                GROUP BY {pk_column}
                HAVING COUNT(*) > 1
            """).df()
            
            if len(result) > 0:
                print_error(f"{table}.{pk_column}: Found {len(result)} duplicate values")
                print(f"  First 5 duplicates: {result[pk_column].head().tolist()}")
                all_valid = False
            else:
                print_success(f"{table}.{pk_column}: No duplicates found")
                
            # Check for NULLs
            null_count = conn.execute(f"""
                SELECT COUNT(*) 
                FROM {table} 
                WHERE {pk_column} IS NULL
            """).fetchone()[0]
            
            if null_count > 0:
                print_error(f"{table}.{pk_column}: Found {null_count} NULL values")
                all_valid = False
                
        except Exception as e:
            print_error(f"{table}.{pk_column}: Error checking - {e}")
            all_valid = False
    
    return all_valid

def check_foreign_keys(conn):
    """Check foreign key constraints"""
    print_header("Foreign Key Validation")
    
    fk_checks = [
        ('product_skus', 'productId', 'products', 'productId'),
        ('orders', 'customerId', 'customers', 'customerId'),
        ('order_items', 'orderId', 'orders', 'orderId'),
        ('order_items', 'productId', 'products', 'productId'),
        ('sales', 'orderId', 'orders', 'orderId'),
        ('sales', 'productId', 'products', 'productId'),
        ('sales', 'customerId', 'customers', 'customerId')
    ]
    
    all_valid = True
    
    for child_table, child_col, parent_table, parent_col in fk_checks:
        try:
            # Check for orphaned records
            orphans = conn.execute(f"""
                SELECT COUNT(*) 
                FROM {child_table} c
                WHERE NOT EXISTS (
                    SELECT 1 FROM {parent_table} p 
                    WHERE p.{parent_col} = c.{child_col}
                )
                AND c.{child_col} IS NOT NULL
            """).fetchone()[0]
            
            if orphans > 0:
                print_error(f"{child_table}.{child_col} -> {parent_table}.{parent_col}: Found {orphans} orphaned records")
                
                # Show sample orphaned values
                sample = conn.execute(f"""
                    SELECT DISTINCT c.{child_col}
                    FROM {child_table} c
                    WHERE NOT EXISTS (
                        SELECT 1 FROM {parent_table} p 
                        WHERE p.{parent_col} = c.{child_col}
                    )
                    AND c.{child_col} IS NOT NULL
                    LIMIT 5
                """).df()
                
                if len(sample) > 0:
                    print(f"  Sample orphaned values: {sample[child_col].tolist()}")
                
                all_valid = False
            else:
                print_success(f"{child_table}.{child_col} -> {parent_table}.{parent_col}: Valid")
                
        except Exception as e:
            print_error(f"{child_table}.{child_col} -> {parent_table}.{parent_col}: Error - {e}")
            all_valid = False
    
    return all_valid

def check_data_integrity(conn):
    """Check data integrity rules"""
    print_header("Data Integrity Validation")
    
    issues = []
    
    # 1. Check price consistency in products
    price_issues = conn.execute("""
        SELECT COUNT(*) 
        FROM products 
        WHERE ABS(price_gbp - price_pence/100.0) > 0.01
    """).fetchone()[0]
    
    if price_issues > 0:
        print_error(f"Products: {price_issues} records with price_gbp != price_pence/100")
        issues.append("price_consistency")
    else:
        print_success("Products: price_gbp and price_pence are consistent")
    
    # 2. Check order_items total price calculation
    item_price_issues = conn.execute("""
        SELECT COUNT(*)
        FROM order_items
        WHERE ABS(totalPrice - (quantity * unitPrice)) > 0.01
    """).fetchone()[0]
    
    if item_price_issues > 0:
        print_error(f"Order items: {item_price_issues} records with incorrect totalPrice")
        issues.append("order_item_totals")
    else:
        print_success("Order items: totalPrice calculations are correct")
    
    # 3. Check orders total amount vs sum of items
    order_total_issues = conn.execute("""
        SELECT COUNT(*)
        FROM (
            SELECT 
                o.orderId,
                o.totalAmount,
                COALESCE(SUM(oi.totalPrice), 0) as calculated_total,
                ABS(o.totalAmount - COALESCE(SUM(oi.totalPrice), 0)) as diff
            FROM orders o
            LEFT JOIN order_items oi ON o.orderId = oi.orderId
            GROUP BY o.orderId, o.totalAmount
            HAVING ABS(o.totalAmount - COALESCE(SUM(oi.totalPrice), 0)) > 0.01
        )
    """).fetchone()[0]
    
    if order_total_issues > 0:
        print_error(f"Orders: {order_total_issues} orders with totalAmount != sum of items")
        issues.append("order_totals")
    else:
        print_success("Orders: totalAmount matches sum of order items")
    
    # 4. Check order status values
    invalid_status = conn.execute("""
        SELECT COUNT(*)
        FROM orders
        WHERE orderStatus NOT IN ('DELIVERED', 'CANCELLED', 'PICKED', 'FUTURE')
    """).fetchone()[0]
    
    if invalid_status > 0:
        print_error(f"Orders: {invalid_status} orders with invalid status")
        issues.append("order_status")
    else:
        print_success("Orders: All order statuses are valid")
    
    # 5. Check sales only for delivered orders
    sales_status_issues = conn.execute("""
        SELECT COUNT(*)
        FROM sales s
        JOIN orders o ON s.orderId = o.orderId
        WHERE o.orderStatus != 'DELIVERED'
    """).fetchone()[0]
    
    if sales_status_issues > 0:
        print_error(f"Sales: {sales_status_issues} sales records for non-delivered orders")
        issues.append("sales_status")
    else:
        print_success("Sales: All sales are for delivered orders")
    
    # 6. Check date consistency
    date_issues = conn.execute("""
        SELECT COUNT(*)
        FROM orders
        WHERE deliveryDate < orderDate
    """).fetchone()[0]
    
    if date_issues > 0:
        print_error(f"Orders: {date_issues} orders with deliveryDate before orderDate")
        issues.append("date_consistency")
    else:
        print_success("Orders: Date consistency is valid")
    
    return len(issues) == 0

def check_duplicates_detailed(conn):
    """Detailed duplicate checking"""
    print_header("Detailed Duplicate Analysis")
    
    # Check for duplicate order items for same product in same order
    dup_order_items = conn.execute("""
        SELECT orderId, productId, COUNT(*) as cnt
        FROM order_items
        GROUP BY orderId, productId
        HAVING COUNT(*) > 1
        ORDER BY cnt DESC
        LIMIT 10
    """).df()
    
    if len(dup_order_items) > 0:
        print_warning(f"Found {len(dup_order_items)} orders with duplicate products")
        print("  This might be intentional (same product ordered multiple times)")
        print(f"  Top cases: \n{dup_order_items}")
    else:
        print_success("No duplicate products within orders")
    
    # Check for duplicate sales records
    dup_sales = conn.execute("""
        SELECT orderId, productId, customerId, saleDate, COUNT(*) as cnt
        FROM sales
        GROUP BY orderId, productId, customerId, saleDate
        HAVING COUNT(*) > 1
    """).df()
    
    if len(dup_sales) > 0:
        print_error(f"Found {len(dup_sales)} duplicate sales records")
        print(f"  Duplicates: \n{dup_sales}")
    else:
        print_success("No duplicate sales records")
    
    # Check for duplicate SKUs for same product
    dup_skus = conn.execute("""
        SELECT productId, COUNT(DISTINCT sku) as sku_count
        FROM product_skus
        GROUP BY productId
        ORDER BY sku_count DESC
        LIMIT 10
    """).df()
    
    print(f"\nProducts with multiple SKUs (top 10):")
    print(dup_skus)

def generate_summary_report(conn):
    """Generate summary statistics"""
    print_header("Database Summary Statistics")
    
    stats = {}
    
    # Get record counts
    tables = ['products', 'product_skus', 'customers', 'orders', 'order_items', 'sales']
    
    for table in tables:
        count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        stats[table] = count
        print(f"{table:15s}: {count:,} records")
    
    # Additional statistics
    print("\nAdditional Statistics:")
    
    # Products with SKUs
    products_with_skus = conn.execute("""
        SELECT COUNT(DISTINCT productId) 
        FROM product_skus
    """).fetchone()[0]
    print(f"Products with SKUs: {products_with_skus:,} / {stats['products']:,}")
    
    # Average SKUs per product
    avg_skus = conn.execute("""
        SELECT AVG(sku_count) 
        FROM (
            SELECT productId, COUNT(*) as sku_count 
            FROM product_skus 
            GROUP BY productId
        )
    """).fetchone()[0]
    print(f"Average SKUs per product: {avg_skus:.2f}")
    
    # Order statistics
    order_stats = conn.execute("""
        SELECT 
            orderStatus,
            COUNT(*) as count,
            AVG(totalAmount) as avg_amount
        FROM orders
        GROUP BY orderStatus
    """).df()
    
    print("\nOrders by status:")
    for _, row in order_stats.iterrows():
        print(f"  {row['orderStatus']:12s}: {row['count']:,} orders (avg: £{row['avg_amount']:.2f})")
    
    # Date range
    date_range = conn.execute("""
        SELECT 
            MIN(orderDate) as first_order,
            MAX(orderDate) as last_order,
            MIN(saleDate) as first_sale,
            MAX(saleDate) as last_sale
        FROM (
            SELECT MIN(orderDate) as orderDate, MIN(saleDate) as saleDate
            FROM orders, sales
        )
    """).fetchone()
    
    print(f"\nDate ranges:")
    print(f"  Orders: {date_range[0]} to {date_range[1]}")
    print(f"  Sales:  {date_range[2]} to {date_range[3]}")

def main():
    print_header("Grocery Database Validation")
    
    # Connect to database
    db_path = Path(__file__).parent.parent / 'data' / 'grocery_final.db'
    conn = duckdb.connect(str(db_path), read_only=True)
    
    try:
        # Run all validations
        schema_valid = validate_schema(conn)
        pk_valid = check_primary_keys(conn)
        fk_valid = check_foreign_keys(conn)
        integrity_valid = check_data_integrity(conn)
        
        # Detailed checks
        check_duplicates_detailed(conn)
        
        # Summary
        generate_summary_report(conn)
        
        # Final verdict
        print_header("Validation Summary")
        
        all_valid = schema_valid and pk_valid and fk_valid and integrity_valid
        
        if all_valid:
            print_success("All validations passed! Database integrity is good.")
        else:
            print_error("Some validations failed. Please review the issues above.")
            
            if not pk_valid:
                print_warning("Primary key issues found - this could cause serious problems")
            if not fk_valid:
                print_warning("Foreign key issues found - referential integrity compromised")
            if not integrity_valid:
                print_warning("Data integrity issues found - calculations may be incorrect")
    
    except Exception as e:
        print_error(f"Error during validation: {e}")
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    main()