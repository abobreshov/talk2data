#!/usr/bin/env python3
"""
Generate purchase orders and inbound deliveries for next 3 days
Based on product ordering calendar and current stock levels
"""

import duckdb
from pathlib import Path
from datetime import datetime, timedelta
import random
import sys
import pandas as pd
import numpy as np

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

DATA_DIR = Path(__file__).parent.parent / 'data'
DB_PATH = DATA_DIR / 'grocery_final.db'

def calculate_order_quantity(product, current_stock, forecast_demand, min_order_qty, order_multiple):
    """Calculate order quantity based on stock levels and demand"""
    # Simple logic: Order enough to cover demand + safety stock
    safety_stock_days = 3
    daily_demand = forecast_demand / 7  # Weekly to daily
    
    # Calculate need
    safety_stock = daily_demand * safety_stock_days
    total_need = forecast_demand + safety_stock - current_stock
    
    # Round up to order multiples
    if total_need <= 0:
        return 0  # No need to order
    
    # Ensure minimum order quantity
    order_qty = max(total_need, min_order_qty)
    
    # Round to order multiple
    if order_multiple > 1:
        order_qty = np.ceil(order_qty / order_multiple) * order_multiple
    
    return int(order_qty)

def generate_sku_for_delivery(productId, delivery_date, batch_num):
    """Generate SKU in format: productId-YYYYMMDD-batch"""
    date_str = delivery_date.strftime('%Y%m%d')
    return f"{productId}-{date_str}-{batch_num:03d}"

def generate_purchase_orders():
    """Generate purchase orders for products needing replenishment"""
    
    print("=== Generating Purchase Orders for Next 3 Days ===\n")
    
    try:
        conn = duckdb.connect(str(DB_PATH))
        
        today = datetime.now().date()
        three_days_ahead = today + timedelta(days=3)
        
        # 1. Get ordering opportunities for next 3 days
        print("1. Loading ordering opportunities...")
        opportunities = conn.execute("""
            SELECT 
                poc.*,
                p.name as product_name,
                p.price_pence,
                s.supplier_name,
                COALESCE(sa.total_quantity, 0) as current_stock,
                COALESCE(sa.expiring_week_qty, 0) as expiring_soon
            FROM product_ordering_calendar poc
            JOIN products p ON poc.productId = p.productId
            JOIN suppliers s ON poc.supplier_id = s.supplier_id
            LEFT JOIN stock_availability sa ON poc.productId = sa.productId
            WHERE poc.order_date >= ?
            AND poc.order_date <= ?
            AND poc.is_active = TRUE
            ORDER BY poc.order_date, poc.supplier_id, poc.productId
        """, [today, three_days_ahead]).fetchdf()
        
        print(f"Found {len(opportunities)} ordering opportunities")
        
        # 2. Get latest forecast data
        print("\n2. Loading forecast data...")
        forecasts = conn.execute("""
            SELECT 
                productId,
                SUM(predicted_quantity) as weekly_demand
            FROM latest_forecasts
            WHERE target_date >= CURRENT_DATE
            AND target_date <= CURRENT_DATE + INTERVAL '7 days'
            GROUP BY productId
        """).fetchdf()
        
        # Create forecast lookup
        forecast_lookup = forecasts.set_index('productId')['weekly_demand'].to_dict()
        
        # 3. Group opportunities by date and supplier to create POs
        print("\n3. Creating purchase orders...")
        
        purchase_orders = []
        inbound_deliveries = []
        po_counter = 1000
        delivery_counter = 1000
        
        # Group by order date and supplier
        grouped = opportunities.groupby(['order_date', 'supplier_id'])
        
        for (order_date, supplier_id), group in grouped:
            po_id = f"PO_{order_date.strftime('%Y%m%d')}_{po_counter:04d}"
            po_counter += 1
            
            # Create PO header
            po_total_items = 0
            po_total_amount = 0
            po_deliveries = []
            
            # Process each product in this PO
            batch_counters = {}  # Track batch numbers per product
            
            for _, opp in group.iterrows():
                productId = opp['productId']
                
                # Get forecast demand (default to historical average if no forecast)
                forecast_demand = forecast_lookup.get(productId, 100)
                
                # Calculate order quantity
                order_qty = calculate_order_quantity(
                    opp,
                    opp['current_stock'],
                    forecast_demand,
                    opp['min_order_quantity'],
                    opp['order_multiple']
                )
                
                if order_qty > 0:
                    # Get batch number for this product
                    if productId not in batch_counters:
                        batch_counters[productId] = 1
                    else:
                        batch_counters[productId] += 1
                    
                    batch_num = batch_counters[productId]
                    
                    # Generate SKU
                    sku = generate_sku_for_delivery(productId, opp['atp_delivery_date'], batch_num)
                    
                    # Calculate expiration date based on purge date logic
                    days_until_purge = (opp['purge_date'] - opp['atp_delivery_date']).days
                    expiration_date = opp['atp_delivery_date'] + timedelta(days=int(days_until_purge * 0.8))
                    
                    # Calculate unit cost (90-110% of selling price)
                    unit_cost = (opp['price_pence'] / 100) * random.uniform(0.6, 0.8)
                    
                    delivery_id = f"DEL_{order_date.strftime('%Y%m%d')}_{delivery_counter:04d}"
                    delivery_counter += 1
                    
                    inbound_delivery = {
                        'delivery_id': delivery_id,
                        'po_id': po_id,
                        'productId': productId,
                        'sku': sku,
                        'quantity': order_qty,
                        'unit_cost': round(unit_cost, 2),
                        'expected_delivery_date': opp['atp_delivery_date'],
                        'actual_delivery_date': None,  # Will be updated on receipt
                        'expiration_date': expiration_date,
                        'batch_number': f"BATCH_{opp['atp_delivery_date'].strftime('%Y%m%d')}_{batch_num:03d}",
                        'status': 'PENDING',
                        'created_at': datetime.now()
                    }
                    
                    inbound_deliveries.append(inbound_delivery)
                    po_deliveries.append(inbound_delivery)
                    
                    po_total_items += order_qty
                    po_total_amount += order_qty * unit_cost
            
            # Create PO if it has items
            if po_deliveries:
                purchase_order = {
                    'po_id': po_id,
                    'supplier_id': supplier_id,
                    'order_date': order_date,
                    'expected_delivery_date': group['atp_delivery_date'].iloc[0],
                    'po_status': 'PENDING',
                    'total_items': po_total_items,
                    'total_amount': round(po_total_amount, 2),
                    'created_at': datetime.now(),
                    'updated_at': datetime.now()
                }
                purchase_orders.append(purchase_order)
        
        print(f"Generated {len(purchase_orders)} purchase orders")
        print(f"Generated {len(inbound_deliveries)} inbound deliveries")
        
        # 4. Insert into database
        print("\n4. Saving to database...")
        
        if purchase_orders:
            # Clear existing data for these dates
            conn.execute("""
                DELETE FROM inbound_deliveries 
                WHERE po_id IN (
                    SELECT po_id FROM purchase_orders 
                    WHERE order_date >= ? AND order_date <= ?
                )
            """, [today, three_days_ahead])
            
            conn.execute("""
                DELETE FROM purchase_orders 
                WHERE order_date >= ? AND order_date <= ?
            """, [today, three_days_ahead])
            
            # Insert new data
            po_df = pd.DataFrame(purchase_orders)
            conn.execute("INSERT INTO purchase_orders SELECT * FROM po_df")
            
            del_df = pd.DataFrame(inbound_deliveries)
            conn.execute("INSERT INTO inbound_deliveries SELECT * FROM del_df")
            
            print(f"✓ Saved {len(purchase_orders)} purchase orders")
            print(f"✓ Saved {len(inbound_deliveries)} inbound deliveries")
        
        # 5. Generate summary report
        print("\n5. Summary Report")
        print("=" * 50)
        
        # POs by date
        po_summary = conn.execute("""
            SELECT 
                order_date,
                COUNT(DISTINCT po_id) as num_pos,
                COUNT(DISTINCT supplier_id) as num_suppliers,
                SUM(total_items) as total_units,
                SUM(total_amount) as total_value
            FROM purchase_orders
            WHERE order_date >= ? AND order_date <= ?
            GROUP BY order_date
            ORDER BY order_date
        """, [today, three_days_ahead]).fetchdf()
        
        print("\nPurchase Orders by Date:")
        print(po_summary)
        
        # Top products being ordered
        top_products = conn.execute("""
            SELECT 
                p.name as product_name,
                COUNT(DISTINCT id.po_id) as num_orders,
                SUM(id.quantity) as total_quantity,
                SUM(id.quantity * id.unit_cost) as total_value
            FROM inbound_deliveries id
            JOIN products p ON id.productId = p.productId
            JOIN purchase_orders po ON id.po_id = po.po_id
            WHERE po.order_date >= ? AND po.order_date <= ?
            GROUP BY p.name
            ORDER BY total_value DESC
            LIMIT 10
        """, [today, three_days_ahead]).fetchdf()
        
        print("\nTop 10 Products by Order Value:")
        print(top_products)
        
        # Deliveries schedule
        delivery_schedule = conn.execute("""
            SELECT 
                expected_delivery_date,
                COUNT(DISTINCT po_id) as num_pos,
                COUNT(*) as num_deliveries,
                SUM(quantity) as total_units
            FROM inbound_deliveries
            WHERE expected_delivery_date >= CURRENT_DATE
            AND expected_delivery_date <= CURRENT_DATE + INTERVAL '7 days'
            GROUP BY expected_delivery_date
            ORDER BY expected_delivery_date
        """).fetchdf()
        
        print("\nUpcoming Delivery Schedule:")
        print(delivery_schedule)
        
        # Update product_skus table with new SKUs
        print("\n6. Updating product_skus table...")
        new_skus = conn.execute("""
            INSERT INTO product_skus (productId, sku, sku_suffix, is_primary, created_at)
            SELECT DISTINCT
                productId,
                sku,
                SUBSTRING(sku FROM LENGTH(productId) + 2) as sku_suffix,
                FALSE as is_primary,
                CURRENT_TIMESTAMP
            FROM inbound_deliveries id
            WHERE NOT EXISTS (
                SELECT 1 FROM product_skus ps 
                WHERE ps.sku = id.sku
            )
        """).fetchone()
        
        # Count new SKUs
        new_sku_count = conn.execute("""
            SELECT COUNT(DISTINCT sku) 
            FROM inbound_deliveries id
            WHERE NOT EXISTS (
                SELECT 1 FROM product_skus ps 
                WHERE ps.sku = id.sku
            )
        """).fetchone()[0]
        
        print(f"✓ Added {new_sku_count} new SKUs to product_skus table")
        
        conn.close()
        print("\n✓ Purchase order generation completed successfully!")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        raise

if __name__ == "__main__":
    generate_purchase_orders()