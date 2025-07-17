#!/usr/bin/env python3
"""
Create and populate product ordering calendar.
Consolidates scripts 20-22 into one comprehensive ordering calendar setup.
"""

import duckdb
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from colorama import init, Fore, Style

# Initialize colorama
init()

DATA_DIR = Path(__file__).parent.parent / 'data'
DB_PATH = DATA_DIR / 'grocery_final.db'

def print_header(text):
    """Print formatted header"""
    print(f"\n{Fore.CYAN}{'='*60}{Style.RESET_ALL}")
    print(f"{Fore.CYAN}{text}{Style.RESET_ALL}")
    print(f"{Fore.CYAN}{'='*60}{Style.RESET_ALL}")

def create_ordering_calendar():
    """Create and populate the product ordering calendar"""
    
    print_header("Creating Product Ordering Calendar")
    print(f"Timestamp: {datetime.now()}")
    
    # Connect to database
    conn = duckdb.connect(str(DB_PATH))
    
    # 1. Create ordering calendar table
    print("\n1. Creating product_ordering_calendar table...")
    conn.execute("""
        CREATE OR REPLACE TABLE product_ordering_calendar (
            calendar_id INTEGER PRIMARY KEY,
            productId VARCHAR NOT NULL,
            supplier_id INTEGER NOT NULL,
            order_date DATE NOT NULL,
            atp_delivery_date DATE NOT NULL,
            purge_date DATE NOT NULL,
            min_order_quantity INTEGER DEFAULT 10,
            order_multiple INTEGER DEFAULT 1,
            lead_time_days INTEGER DEFAULT 3,
            is_active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (productId) REFERENCES products(productId),
            FOREIGN KEY (supplier_id) REFERENCES suppliers(supplier_id)
        )
    """)
    print(f"  {Fore.GREEN}✓{Style.RESET_ALL} Table created")
    
    # 2. Load necessary data
    print("\n2. Loading data...")
    
    # Get products with suppliers
    products = conn.execute("""
        SELECT 
            p.productId,
            p.name,
            p.category,
            p.subcategory,
            s.supplier_id,
            s.supplier_name
        FROM products p
        INNER JOIN suppliers s ON p.brandName = s.supplier_name
        ORDER BY p.productId
    """).fetchdf()
    print(f"  Found {len(products):,} product-supplier mappings")
    
    # Get supplier schedules
    supplier_schedules = conn.execute("""
        SELECT 
            supplier_id,
            delivery_date,
            po_cutoff_date,
            lead_time_days
        FROM supplier_schedules
        WHERE delivery_date >= CURRENT_DATE
        ORDER BY supplier_id, delivery_date
    """).fetchdf()
    print(f"  Found {len(supplier_schedules):,} supplier schedule entries")
    
    # Get product lifecycle info
    product_lifecycle = conn.execute("""
        SELECT 
            category,
            subcategory,
            shelf_life_days,
            min_shelf_life_days,
            max_shelf_life_days
        FROM product_purge_reference
    """).fetchdf()
    
    # 3. Generate ordering calendar entries
    print("\n3. Generating ordering calendar entries...")
    
    calendar_entries = []
    calendar_id = 1
    
    # For each product
    for _, product in products.iterrows():
        product_id = product['productId']
        supplier_id = product['supplier_id']
        category = product['category']
        subcategory = product['subcategory']
        
        # Get shelf life for this product
        lifecycle_match = product_lifecycle[
            (product_lifecycle['category'] == category) & 
            (product_lifecycle['subcategory'] == subcategory)
        ]
        
        if lifecycle_match.empty:
            # Default shelf life by category
            shelf_life_days = 14  # Default
        else:
            shelf_life_days = int(lifecycle_match.iloc[0]['shelf_life_days'])
        
        # Get supplier delivery dates
        supplier_dates = supplier_schedules[
            supplier_schedules['supplier_id'] == supplier_id
        ]
        
        # Create ordering opportunities for next 30 days
        for _, schedule in supplier_dates.iterrows():
            if len(calendar_entries) >= 50000:  # Limit for performance
                break
                
            delivery_date = schedule['delivery_date']
            po_cutoff_date = schedule['po_cutoff_date']
            lead_time = schedule['lead_time_days']
            
            # Calculate purge date
            purge_date = delivery_date + timedelta(days=shelf_life_days)
            
            # Determine order quantity parameters
            if category in ['Fresh Food', 'Fruit & Vegetables']:
                min_qty = np.random.randint(20, 40)
                order_multiple = 5
            elif category in ['Chilled Food', 'Dairy']:
                min_qty = np.random.randint(15, 30)
                order_multiple = 3
            else:
                min_qty = np.random.randint(10, 25)
                order_multiple = 1
            
            calendar_entry = {
                'calendar_id': calendar_id,
                'productId': product_id,
                'supplier_id': supplier_id,
                'order_date': po_cutoff_date,
                'atp_delivery_date': delivery_date,
                'purge_date': purge_date,
                'min_order_quantity': min_qty,
                'order_multiple': order_multiple,
                'lead_time_days': lead_time,
                'is_active': True
            }
            
            calendar_entries.append(calendar_entry)
            calendar_id += 1
    
    # 4. Insert calendar entries
    print(f"  Generated {len(calendar_entries):,} calendar entries")
    print("\n4. Inserting into database...")
    
    if calendar_entries:
        calendar_df = pd.DataFrame(calendar_entries)
        conn.execute("INSERT INTO product_ordering_calendar SELECT * FROM calendar_df")
        print(f"  {Fore.GREEN}✓{Style.RESET_ALL} Inserted {len(calendar_entries):,} entries")
    
    # 5. Create indexes for performance
    print("\n5. Creating indexes...")
    indexes = [
        ("idx_poc_product", "productId"),
        ("idx_poc_supplier", "supplier_id"),
        ("idx_poc_order_date", "order_date"),
        ("idx_poc_delivery_date", "atp_delivery_date"),
        ("idx_poc_active", "is_active")
    ]
    
    for idx_name, column in indexes:
        try:
            conn.execute(f"CREATE INDEX {idx_name} ON product_ordering_calendar({column})")
            print(f"  {Fore.GREEN}✓{Style.RESET_ALL} Created index {idx_name}")
        except:
            print(f"  {Fore.YELLOW}-{Style.RESET_ALL} Index {idx_name} already exists")
    
    # 6. Show summary
    print("\n6. Summary:")
    
    # Calendar statistics
    stats = conn.execute("""
        SELECT 
            COUNT(DISTINCT productId) as unique_products,
            COUNT(DISTINCT supplier_id) as unique_suppliers,
            COUNT(*) as total_entries,
            MIN(order_date) as earliest_order,
            MAX(order_date) as latest_order,
            AVG(lead_time_days) as avg_lead_time
        FROM product_ordering_calendar
        WHERE is_active = TRUE
    """).fetchone()
    
    print(f"  - Unique products: {stats[0]:,}")
    print(f"  - Unique suppliers: {stats[1]:,}")
    print(f"  - Total entries: {stats[2]:,}")
    print(f"  - Date range: {stats[3]} to {stats[4]}")
    print(f"  - Average lead time: {stats[5]:.1f} days")
    
    # Sample entries
    print("\n7. Sample ordering opportunities:")
    samples = conn.execute("""
        SELECT 
            p.name,
            s.supplier_name,
            poc.order_date,
            poc.atp_delivery_date,
            poc.min_order_quantity
        FROM product_ordering_calendar poc
        JOIN products p ON poc.productId = p.productId
        JOIN suppliers s ON poc.supplier_id = s.supplier_id
        WHERE poc.is_active = TRUE
        ORDER BY poc.order_date
        LIMIT 5
    """).fetchall()
    
    for name, supplier, order_dt, delivery_dt, min_qty in samples:
        print(f"  - {name[:30]:<30} | {supplier[:20]:<20} | Order: {order_dt} | Delivery: {delivery_dt} | Min: {min_qty}")
    
    conn.close()
    print(f"\n✓ Product ordering calendar created successfully")

if __name__ == "__main__":
    create_ordering_calendar()