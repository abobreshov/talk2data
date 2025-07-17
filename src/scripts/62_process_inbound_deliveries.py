#!/usr/bin/env python3
"""
Process inbound deliveries and update stock table
Simulates receiving deliveries that are due today
"""

import duckdb
from pathlib import Path
from datetime import datetime, timedelta
import random
import sys

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

DATA_DIR = Path(__file__).parent.parent / 'data'
DB_PATH = DATA_DIR / 'grocery_final.db'

def process_inbound_deliveries():
    """Process pending deliveries and add to stock"""
    
    print("=== Processing Inbound Deliveries to Stock ===\n")
    
    try:
        conn = duckdb.connect(str(DB_PATH))
        
        today = datetime.now().date()
        
        # 1. Get deliveries due today or earlier that are still pending
        print("1. Loading pending deliveries...")
        pending_deliveries = conn.execute("""
            SELECT 
                id.*,
                p.name as product_name,
                p.category,
                p.subcategory,
                po.supplier_id
            FROM inbound_deliveries id
            JOIN products p ON id.productId = p.productId
            JOIN purchase_orders po ON id.po_id = po.po_id
            WHERE id.status = 'PENDING'
            AND id.expected_delivery_date <= ?
            ORDER BY id.expected_delivery_date, id.productId
        """, [today]).fetchdf()
        
        print(f"Found {len(pending_deliveries)} deliveries to process")
        
        if len(pending_deliveries) == 0:
            print("No pending deliveries to process")
            return
        
        # 2. Process each delivery
        print("\n2. Processing deliveries...")
        
        stock_entries = []
        processed_deliveries = []
        stock_id_counter = conn.execute("SELECT COALESCE(MAX(stock_id), 0) + 1 FROM stock").fetchone()[0]
        
        for _, delivery in pending_deliveries.iterrows():
            # Simulate 98% successful delivery rate
            if random.random() < 0.98:
                # Successful delivery
                stock_entry = {
                    'stock_id': stock_id_counter,
                    'sku': delivery['sku'],
                    'productId': delivery['productId'],
                    'quantity_in_stock': delivery['quantity'],
                    'expiration_date': delivery['expiration_date'],
                    'batch_number': delivery['batch_number'],
                    'received_date': today,
                    'temperature_zone': determine_temperature_zone(delivery['category'], delivery['subcategory']),
                    'stock_status': 'AVAILABLE',
                    'last_updated': datetime.now(),
                    'purchase_price': delivery['unit_cost'],
                    'supplier_id': delivery['supplier_id']
                }
                
                stock_entries.append(stock_entry)
                stock_id_counter += 1
                
                # Mark delivery as received
                processed_deliveries.append({
                    'delivery_id': delivery['delivery_id'],
                    'actual_delivery_date': today,
                    'status': 'RECEIVED'
                })
            else:
                # Failed delivery (rejected, damaged, etc.)
                processed_deliveries.append({
                    'delivery_id': delivery['delivery_id'],
                    'actual_delivery_date': today,
                    'status': 'REJECTED'
                })
        
        # 3. Update database
        print("\n3. Updating database...")
        
        # Insert new stock entries
        if stock_entries:
            import pandas as pd
            stock_df = pd.DataFrame(stock_entries)
            conn.execute("INSERT INTO stock SELECT * FROM stock_df")
            print(f"✓ Added {len(stock_entries)} items to stock")
        
        # Update delivery statuses
        for delivery in processed_deliveries:
            conn.execute("""
                UPDATE inbound_deliveries 
                SET actual_delivery_date = ?,
                    status = ?
                WHERE delivery_id = ?
            """, [delivery['actual_delivery_date'], delivery['status'], delivery['delivery_id']])
        
        print(f"✓ Updated {len(processed_deliveries)} delivery statuses")
        
        # Update PO statuses
        conn.execute("""
            UPDATE purchase_orders
            SET po_status = CASE
                WHEN NOT EXISTS (
                    SELECT 1 FROM inbound_deliveries 
                    WHERE po_id = purchase_orders.po_id 
                    AND status = 'PENDING'
                ) THEN 'DELIVERED'
                ELSE po_status
            END,
            updated_at = CURRENT_TIMESTAMP
            WHERE po_id IN (
                SELECT DISTINCT po_id 
                FROM inbound_deliveries 
                WHERE delivery_id IN (
                    SELECT delivery_id FROM inbound_deliveries
                    WHERE status IN ('RECEIVED', 'REJECTED')
                    AND actual_delivery_date = ?
                )
            )
        """, [today])
        
        # 4. Generate summary report
        print("\n4. Summary Report")
        print("=" * 50)
        
        # Stock summary by category
        stock_summary = conn.execute("""
            SELECT 
                p.category,
                COUNT(DISTINCT s.productId) as num_products,
                COUNT(DISTINCT s.sku) as num_skus,
                SUM(s.quantity_in_stock) as total_quantity,
                SUM(s.quantity_in_stock * s.purchase_price) as inventory_value
            FROM stock s
            JOIN products p ON s.productId = p.productId
            WHERE s.stock_status = 'AVAILABLE'
            GROUP BY p.category
            ORDER BY inventory_value DESC
        """).fetchdf()
        
        print("\nCurrent Stock by Category:")
        print(stock_summary)
        
        # Deliveries processed today
        today_summary = conn.execute("""
            SELECT 
                status,
                COUNT(*) as count,
                SUM(quantity) as total_units,
                SUM(quantity * unit_cost) as total_value
            FROM inbound_deliveries
            WHERE actual_delivery_date = ?
            GROUP BY status
        """, [today]).fetchdf()
        
        print(f"\nDeliveries Processed Today ({today}):")
        print(today_summary)
        
        # Upcoming deliveries
        upcoming = conn.execute("""
            SELECT 
                expected_delivery_date,
                COUNT(*) as num_deliveries,
                SUM(quantity) as total_units
            FROM inbound_deliveries
            WHERE status = 'PENDING'
            AND expected_delivery_date > CURRENT_DATE
            AND expected_delivery_date <= CURRENT_DATE + INTERVAL '7 days'
            GROUP BY expected_delivery_date
            ORDER BY expected_delivery_date
        """).fetchdf()
        
        print("\nUpcoming Deliveries (Next 7 Days):")
        print(upcoming)
        
        # Products with low stock
        low_stock = conn.execute("""
            SELECT 
                p.name as product_name,
                COALESCE(sa.total_quantity, 0) as current_stock,
                COALESCE(sa.expiring_week_qty, 0) as expiring_soon,
                COUNT(pd.productId) as pending_deliveries
            FROM products p
            LEFT JOIN stock_availability sa ON p.productId = sa.productId
            LEFT JOIN pending_deliveries pd ON p.productId = pd.productId
            WHERE COALESCE(sa.total_quantity, 0) < 50
            ORDER BY current_stock
            LIMIT 10
        """).fetchdf()
        
        print("\nProducts with Low Stock:")
        print(low_stock)
        
        conn.close()
        print("\n✓ Inbound delivery processing completed successfully!")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        raise

def determine_temperature_zone(category, subcategory):
    """Determine temperature zone based on product category"""
    # Chilled products
    chilled_categories = ['Chilled Food', 'Dairy']
    chilled_subcategories = ['Milk', 'Cheese', 'Yoghurt', 'Chilled Meats', 'Ready Meals']
    
    # Frozen products
    frozen_categories = ['Frozen Food']
    frozen_subcategories = ['Ice Cream', 'Frozen Desserts', 'Frozen Meat', 'Frozen Fish']
    
    if category in chilled_categories or subcategory in chilled_subcategories:
        return 'CHILLED'
    elif category in frozen_categories or subcategory in frozen_subcategories:
        return 'FROZEN'
    else:
        return 'AMBIENT'

if __name__ == "__main__":
    process_inbound_deliveries()