#!/usr/bin/env python3
"""
Validate all databases and check their integrity.
Consolidates various check scripts into one comprehensive validation.
"""

import duckdb
from pathlib import Path
import sys
from datetime import datetime
from colorama import init, Fore, Style

# Initialize colorama
init()

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

DATA_DIR = Path(__file__).parent.parent / 'data'

def print_header(text):
    """Print formatted header"""
    print(f"\n{Fore.CYAN}{'='*60}{Style.RESET_ALL}")
    print(f"{Fore.CYAN}{text}{Style.RESET_ALL}")
    print(f"{Fore.CYAN}{'='*60}{Style.RESET_ALL}")

def check_database_exists(db_path, db_name):
    """Check if database file exists"""
    if db_path.exists():
        size_mb = db_path.stat().st_size / (1024 * 1024)
        print(f"{Fore.GREEN}✓{Style.RESET_ALL} {db_name} exists ({size_mb:.2f} MB)")
        return True
    else:
        print(f"{Fore.RED}✗{Style.RESET_ALL} {db_name} not found")
        return False

def check_database_schema(db_path, expected_tables):
    """Check if database has expected tables"""
    try:
        conn = duckdb.connect(str(db_path), read_only=True)
        
        # Get all tables
        tables = conn.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_type = 'BASE TABLE'
            ORDER BY table_name
        """).fetchall()
        
        existing_tables = {t[0] for t in tables}
        
        print(f"\nFound {len(existing_tables)} tables:")
        for table in sorted(existing_tables):
            row_count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            print(f"  - {table}: {row_count:,} rows")
        
        # Check for missing tables
        missing_tables = set(expected_tables) - existing_tables
        if missing_tables:
            print(f"\n{Fore.YELLOW}Missing tables:{Style.RESET_ALL}")
            for table in sorted(missing_tables):
                print(f"  - {table}")
        
        conn.close()
        return len(missing_tables) == 0
        
    except Exception as e:
        print(f"{Fore.RED}Error checking schema: {e}{Style.RESET_ALL}")
        return False

def check_data_integrity(db_path):
    """Run basic data integrity checks"""
    try:
        conn = duckdb.connect(str(db_path), read_only=True)
        issues = []
        
        # Check for orphaned records
        integrity_checks = [
            ("Orphaned order items", """
                SELECT COUNT(*) FROM order_items oi 
                LEFT JOIN orders o ON oi.orderId = o.orderId 
                WHERE o.orderId IS NULL
            """),
            ("Orphaned sales", """
                SELECT COUNT(*) FROM sales s 
                LEFT JOIN orders o ON s.orderId = o.orderId 
                WHERE o.orderId IS NULL
            """),
            ("Products without SKUs", """
                SELECT COUNT(*) FROM products p 
                LEFT JOIN product_skus ps ON p.productId = ps.productId 
                WHERE ps.productId IS NULL
            """),
            ("Orders without items", """
                SELECT COUNT(*) FROM orders o 
                LEFT JOIN order_items oi ON o.orderId = oi.orderId 
                WHERE oi.orderId IS NULL
            """),
        ]
        
        print("\nRunning integrity checks:")
        for check_name, query in integrity_checks:
            try:
                count = conn.execute(query).fetchone()[0]
                if count > 0:
                    print(f"  {Fore.YELLOW}⚠{Style.RESET_ALL}  {check_name}: {count}")
                    issues.append(f"{check_name}: {count}")
                else:
                    print(f"  {Fore.GREEN}✓{Style.RESET_ALL} {check_name}: OK")
            except:
                # Table might not exist
                print(f"  {Fore.GRAY}-{Style.RESET_ALL} {check_name}: N/A")
        
        conn.close()
        return len(issues) == 0, issues
        
    except Exception as e:
        print(f"{Fore.RED}Error checking integrity: {e}{Style.RESET_ALL}")
        return False, [str(e)]

def validate_all_databases():
    """Validate all databases in the project"""
    print_header("Database Validation")
    print(f"Timestamp: {datetime.now()}")
    
    databases = [
        {
            "path": DATA_DIR / "products.duckdb",
            "name": "Products Database",
            "tables": ["products", "product_skus"]
        },
        {
            "path": DATA_DIR / "orders.duckdb",
            "name": "Orders Database", 
            "tables": ["orders", "order_items", "sales"]
        },
        {
            "path": DATA_DIR / "grocery_final.db",
            "name": "Final Consolidated Database",
            "tables": [
                "products", "product_skus", "customers", "orders", 
                "order_items", "sales", "suppliers", "supplier_schedules",
                "stock", "forecasts", "purchase_orders", "inbound_deliveries"
            ]
        }
    ]
    
    all_valid = True
    
    for db_info in databases:
        print_header(db_info["name"])
        
        # Check existence
        if not check_database_exists(db_info["path"], db_info["name"]):
            all_valid = False
            continue
        
        # Check schema
        schema_valid = check_database_schema(db_info["path"], db_info["tables"])
        if not schema_valid:
            all_valid = False
        
        # Check integrity (only for final database)
        if "final" in db_info["name"].lower():
            integrity_valid, issues = check_data_integrity(db_info["path"])
            if not integrity_valid:
                all_valid = False
    
    # Summary
    print_header("Validation Summary")
    if all_valid:
        print(f"{Fore.GREEN}✓ All databases are valid and ready{Style.RESET_ALL}")
    else:
        print(f"{Fore.RED}✗ Some issues found - please review above{Style.RESET_ALL}")
    
    return all_valid

if __name__ == "__main__":
    validate_all_databases()