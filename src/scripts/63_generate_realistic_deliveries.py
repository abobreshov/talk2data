#!/usr/bin/env python3
"""
Generate realistic inbound deliveries based on actual demand.
Purges existing unrealistic deliveries and creates new ones with proper volumes.
"""

import duckdb
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from tqdm import tqdm
import random
from colorama import init, Fore, Style

# Initialize colorama
init()

def print_header(text):
    """Print formatted header"""
    print(f"\n{Fore.CYAN}{'='*60}{Style.RESET_ALL}")
    print(f"{Fore.CYAN}{text}{Style.RESET_ALL}")
    print(f"{Fore.CYAN}{'='*60}{Style.RESET_ALL}")

def purge_existing_deliveries(conn):
    """Purge all existing unrealistic deliveries"""
    print_header("Purging Existing Deliveries and Purchase Orders")
    
    # Get counts before purge
    delivery_count = conn.execute("SELECT COUNT(*) FROM inbound_deliveries").fetchone()[0]
    total_units = conn.execute("SELECT SUM(quantity) FROM inbound_deliveries").fetchone()[0] or 0
    po_count = conn.execute("SELECT COUNT(*) FROM purchase_orders").fetchone()[0]
    
    print(f"Found {delivery_count:,} existing deliveries with {total_units:,} units")
    print(f"Found {po_count:,} existing purchase orders")
    
    # Purge all deliveries first (due to foreign key constraints)
    conn.execute("DELETE FROM inbound_deliveries")
    conn.commit()  # Commit immediately to release foreign key constraints
    
    # Then purge all purchase orders
    conn.execute("DELETE FROM purchase_orders")
    conn.commit()  # Commit immediately
    
    # Verify purge
    count_after = conn.execute("SELECT COUNT(*) FROM inbound_deliveries").fetchone()[0]
    po_after = conn.execute("SELECT COUNT(*) FROM purchase_orders").fetchone()[0]
    
    print(f"{Fore.GREEN}✓ Purged {delivery_count:,} delivery records{Style.RESET_ALL}")
    print(f"{Fore.GREEN}✓ Purged {po_count:,} purchase order records{Style.RESET_ALL}")
    
    return delivery_count

def get_product_category_info(conn):
    """Get product categories for delivery size determination"""
    query = """
    SELECT 
        productId,
        name,
        category,
        subcategory,
        CASE 
            WHEN category IN ('Fruit & Vegetables') AND name LIKE '%Banana%' THEN 'high_volume'
            WHEN category IN ('Bakery', 'Dairy') THEN 'medium_volume'
            WHEN category IN ('Food Cupboard') AND subcategory LIKE '%Spice%' THEN 'low_volume'
            WHEN category IN ('Drinks') THEN 'medium_volume'
            ELSE 'medium_volume'
        END as volume_category
    FROM products
    """
    return conn.execute(query).fetchdf().set_index('productId')

def get_delivery_size_range(volume_category):
    """Get appropriate delivery size range based on product category"""
    ranges = {
        'low_volume': (10, 20),
        'medium_volume': (20, 40),
        'high_volume': (50, 100)
    }
    return ranges.get(volume_category, (20, 40))

def assign_supplier_performance():
    """Assign performance ratings to suppliers"""
    return {
        1: {'name': 'Fresh Direct', 'late_rate': 0.06, 'rating': 'excellent', 'lead_time': 2},  # 6% late
        2: {'name': 'Daily Essentials', 'late_rate': 0.13, 'rating': 'poor', 'lead_time': 3},  # 13% late
        3: {'name': 'Metro Wholesale', 'late_rate': 0.09, 'rating': 'good', 'lead_time': 2},   # 9% late
        4: {'name': 'Regional Suppliers', 'late_rate': 0.11, 'rating': 'average', 'lead_time': 3}, # 11% late
        5: {'name': 'Global Foods', 'late_rate': 0.08, 'rating': 'good', 'lead_time': 4},      # 8% late
        6: {'name': 'Express Logistics', 'late_rate': 0.12, 'rating': 'poor', 'lead_time': 2}   # 12% late
    }

def calculate_late_delivery(expected_date, supplier_performance):
    """Calculate actual delivery date based on supplier performance"""
    if random.random() < supplier_performance['late_rate']:
        # Delivery is late
        delay_prob = random.random()
        if delay_prob < 0.65:  # 65% chance of 1 day late
            delay_days = 1
        elif delay_prob < 0.95:  # 30% chance of 2 days late
            delay_days = 2
        else:  # 5% chance of 3 days late
            delay_days = 3
        
        actual_date = expected_date + timedelta(days=delay_days)
        is_late = True
    else:
        actual_date = expected_date
        is_late = False
    
    return actual_date, is_late

def create_purchase_order(conn, po_id, supplier_id, order_date, expected_delivery_date, items, total_amount):
    """Create a purchase order record"""
    conn.execute("""
        INSERT INTO purchase_orders (
            po_id, supplier_id, order_date, expected_delivery_date,
            po_status, total_items, total_amount, created_at, updated_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
    """, [
        po_id, supplier_id, order_date, expected_delivery_date,
        'DELIVERED', items, total_amount, datetime.now(), datetime.now()
    ])

def generate_historical_deliveries(conn, start_date, end_date):
    """Generate historical deliveries based on sales patterns"""
    print_header("Generating Historical Deliveries")
    
    # Get product info
    product_info = get_product_category_info(conn)
    supplier_performance = assign_supplier_performance()
    
    # Get all products to generate deliveries for
    print("Getting product list...")
    all_products = conn.execute("""
        SELECT 
            productId,
            name,
            category,
            subcategory
        FROM products
        ORDER BY productId
    """).fetchdf()
    
    print(f"Found {len(all_products):,} products")
    
    # Get average daily sales per product for sizing deliveries
    print("Analyzing historical demand patterns...")
    avg_daily_demand = conn.execute(f"""
        SELECT 
            productId,
            AVG(daily_qty) as avg_daily_demand
        FROM (
            SELECT 
                productId,
                DATE_TRUNC('day', saleDate) as sale_date,
                SUM(quantity) as daily_qty
            FROM sales
            WHERE saleDate >= '{start_date}'
            AND saleDate < '{end_date}'
            GROUP BY productId, DATE_TRUNC('day', saleDate)
        )
        GROUP BY productId
    """).fetchdf().set_index('productId')['avg_daily_demand'].to_dict()
    
    # Generate deliveries
    deliveries = []
    delivery_stats = {'total': 0, 'late': 0, 'by_supplier': {}}
    
    print("Generating delivery records...")
    
    # Calculate total days
    total_days = (datetime.strptime(end_date, '%Y-%m-%d').date() - 
                  datetime.strptime(start_date, '%Y-%m-%d').date()).days
    
    # Target: average 2-3 deliveries per product per month (realistic for grocery)
    deliveries_per_product_per_month = 2.5
    months_in_period = total_days / 30
    expected_deliveries_per_product = int(deliveries_per_product_per_month * months_in_period)
    
    # Sample products to deliver (80% of products get deliveries)
    products_to_deliver = all_products.sample(frac=0.8, random_state=42)
    
    print(f"Generating deliveries for {len(products_to_deliver):,} products...")
    print(f"Target: ~{expected_deliveries_per_product} deliveries per product over {months_in_period:.1f} months")
    
    # For each product, generate deliveries spread across the period
    for _, product in tqdm(products_to_deliver.iterrows(), total=len(products_to_deliver), desc="Processing products"):
        product_id = product['productId']
        
        # Get average daily demand for this product
        daily_demand = avg_daily_demand.get(product_id, 3.5)  # Default to forecast average
        
        # Get product volume category
        if product_id in product_info.index:
            volume_cat = product_info.loc[product_id]['volume_category']
        else:
            volume_cat = 'medium_volume'
        
        # Number of deliveries for this product
        num_deliveries = random.randint(
            max(1, expected_deliveries_per_product - 1),
            expected_deliveries_per_product + 1
        )
        
        # Spread deliveries across the period
        days_between_deliveries = max(7, total_days // num_deliveries)
        
        # Generate deliveries
        for i in range(num_deliveries):
            # Random supplier
            supplier_id = random.randint(1, 6)
            supplier_perf = supplier_performance[supplier_id]
            
            # Expected delivery date (spread evenly with some randomness)
            base_offset = i * days_between_deliveries
            random_offset = random.randint(-3, 3)  # +/- 3 days variation
            days_from_start = max(0, min(total_days - 1, base_offset + random_offset))
            expected_date = datetime.strptime(start_date, '%Y-%m-%d').date() + timedelta(days=days_from_start)
            
            # Delivery size based on demand and category
            min_size, max_size = get_delivery_size_range(volume_cat)
            
            # Size based on demand (cover 7-14 days of demand)
            days_coverage = random.randint(7, 14)
            ideal_qty = int(daily_demand * days_coverage)
            delivery_qty = max(min_size, min(max_size, ideal_qty))
            
            # Calculate actual delivery date
            actual_date, is_late = calculate_late_delivery(expected_date, supplier_perf)
            
            # Create PO ID
            po_id = f"PO-{expected_date.strftime('%Y%m%d')}-{supplier_id}-{random.randint(1000,9999)}"
            
            # Create delivery record
            delivery_id = f"DEL-{po_id}-{product_id}"
            sku = f"{product_id}-{actual_date.strftime('%Y%m%d')}-{i+1:03d}"
            
            unit_cost = round(random.uniform(0.5, 5.0), 2)
            
            delivery = {
                'delivery_id': delivery_id,
                'po_id': po_id,
                'productId': product_id,
                'sku': sku,
                'quantity': delivery_qty,
                'unit_cost': unit_cost,
                'expected_delivery_date': expected_date,
                'actual_delivery_date': actual_date,
                'expiration_date': actual_date + timedelta(days=random.randint(7, 30)),
                'batch_number': f"BATCH-{actual_date.strftime('%Y%m%d')}-{random.randint(100,999)}",
                'status': 'DELIVERED',
                'created_at': datetime.now(),
                # Additional fields for PO creation
                'supplier_id': supplier_id,
                'order_date': expected_date - timedelta(days=supplier_perf.get('lead_time', 3)),
                'total_amount': delivery_qty * unit_cost
            }
            
            deliveries.append(delivery)
            delivery_stats['total'] += 1
            
            if is_late:
                delivery_stats['late'] += 1
            
            # Track by supplier
            if supplier_id not in delivery_stats['by_supplier']:
                delivery_stats['by_supplier'][supplier_id] = {
                    'name': supplier_perf['name'],
                    'total': 0,
                    'late': 0
                }
            delivery_stats['by_supplier'][supplier_id]['total'] += 1
            if is_late:
                delivery_stats['by_supplier'][supplier_id]['late'] += 1
    
    return deliveries, delivery_stats

def generate_future_deliveries(conn, start_date, num_days):
    """Generate future deliveries based on forecast and current stock"""
    print_header("Generating Future Deliveries")
    
    end_date = start_date + timedelta(days=num_days-1)
    product_info = get_product_category_info(conn)
    supplier_performance = assign_supplier_performance()
    
    # Get forecast demand and current stock
    print("Analyzing forecast demand and stock levels...")
    needs = conn.execute(f"""
        WITH forecast_demand AS (
            SELECT 
                f.productId,
                p.name as product_name,
                SUM(f.predicted_quantity) as total_demand
            FROM forecasts f
            JOIN products p ON f.productId = p.productId
            WHERE f.target_date >= '{start_date}'
            AND f.target_date <= '{end_date}'
            AND f.run_id = (SELECT MAX(run_id) FROM forecasts)
            GROUP BY f.productId, p.name
        ),
        current_stock AS (
            SELECT 
                productId,
                SUM(quantity_in_stock) as stock_qty
            FROM stock
            WHERE quantity_in_stock > 0
            GROUP BY productId
        )
        SELECT 
            fd.productId,
            fd.product_name,
            fd.total_demand,
            COALESCE(cs.stock_qty, 0) as current_stock,
            CASE 
                WHEN fd.total_demand > COALESCE(cs.stock_qty, 0) * 1.2
                THEN fd.total_demand - COALESCE(cs.stock_qty, 0)
                ELSE 0 
            END as units_needed
        FROM forecast_demand fd
        LEFT JOIN current_stock cs ON fd.productId = cs.productId
        WHERE fd.total_demand > COALESCE(cs.stock_qty, 0) * 1.2  -- Need 20% buffer
        ORDER BY (fd.total_demand - COALESCE(cs.stock_qty, 0)) DESC
    """).fetchdf()
    
    print(f"Found {len(needs)} products needing replenishment")
    print(f"Total units needed: {needs['units_needed'].sum():,.0f}")
    
    # Generate deliveries
    deliveries = []
    
    for _, row in needs.iterrows():
        product_id = row['productId']
        units_needed = row['units_needed']
        
        if units_needed <= 0:
            continue
        
        # Get product info
        if product_id in product_info.index:
            prod_info = product_info.loc[product_id]
            volume_cat = prod_info['volume_category']
        else:
            volume_cat = 'medium_volume'
        
        # Determine delivery size
        min_size, max_size = get_delivery_size_range(volume_cat)
        
        # Cover 70-80% of need
        target_coverage = random.uniform(0.7, 0.8)
        total_to_deliver = int(units_needed * target_coverage)
        
        # Split into multiple deliveries
        remaining = total_to_deliver
        delivery_count = 0
        
        while remaining > 0 and delivery_count < 3:  # Max 3 deliveries per product
            # Random supplier
            supplier_id = random.randint(1, 6)
            supplier_perf = supplier_performance[supplier_id]
            
            # Delivery size
            delivery_qty = min(remaining, random.randint(min_size, max_size))
            delivery_qty = max(5, delivery_qty)  # Minimum 5 units
            
            # Expected delivery date (spread across the days)
            days_offset = random.randint(0, num_days-1)
            expected_date = start_date + timedelta(days=days_offset)
            
            # Calculate actual delivery date
            actual_date, is_late = calculate_late_delivery(expected_date, supplier_perf)
            
            # Create PO ID
            po_id = f"PO-{expected_date.strftime('%Y%m%d')}-{supplier_id}-{random.randint(1000,9999)}"
            
            # Create delivery record
            delivery_id = f"DEL-{po_id}-{product_id}"
            sku = f"{product_id}-{expected_date.strftime('%Y%m%d')}-001"
            
            unit_cost = round(random.uniform(0.5, 5.0), 2)
            
            delivery = {
                'delivery_id': delivery_id,
                'po_id': po_id,
                'productId': product_id,
                'sku': sku,
                'quantity': delivery_qty,
                'unit_cost': unit_cost,
                'expected_delivery_date': expected_date,
                'actual_delivery_date': None if actual_date > datetime.now().date() else actual_date,
                'expiration_date': expected_date + timedelta(days=random.randint(7, 30)),
                'batch_number': f"BATCH-{expected_date.strftime('%Y%m%d')}-{random.randint(100,999)}",
                'status': 'PENDING' if actual_date > datetime.now().date() else 'DELIVERED',
                'created_at': datetime.now(),
                # Additional fields for PO creation
                'supplier_id': supplier_id,
                'order_date': expected_date - timedelta(days=supplier_perf.get('lead_time', 3)),
                'total_amount': delivery_qty * unit_cost
            }
            
            deliveries.append(delivery)
            remaining -= delivery_qty
            delivery_count += 1
    
    return deliveries

def insert_deliveries(conn, deliveries):
    """Insert delivery records into database"""
    if not deliveries:
        print("No deliveries to insert")
        return
    
    print(f"\nProcessing {len(deliveries):,} delivery records...")
    
    # Convert to DataFrame
    df = pd.DataFrame(deliveries)
    
    # Group by PO to create purchase orders first
    po_groups = df.groupby('po_id')
    purchase_orders = []
    
    print("Creating purchase orders...")
    for po_id, group in po_groups:
        # Get supplier info from first delivery in group
        first_delivery = group.iloc[0]
        
        purchase_order = {
            'po_id': po_id,
            'supplier_id': first_delivery['supplier_id'],
            'order_date': first_delivery['order_date'],
            'expected_delivery_date': first_delivery['expected_delivery_date'],
            'po_status': 'DELIVERED',  # Historical orders are delivered
            'total_items': int(group['quantity'].sum()),
            'total_amount': float(group['total_amount'].sum()),
            'created_at': datetime.now(),
            'updated_at': datetime.now()
        }
        purchase_orders.append(purchase_order)
    
    # Insert purchase orders
    print(f"Inserting {len(purchase_orders):,} purchase orders...")
    po_df = pd.DataFrame(purchase_orders)
    
    conn.executemany("""
        INSERT INTO purchase_orders (
            po_id, supplier_id, order_date, expected_delivery_date,
            po_status, total_items, total_amount, created_at, updated_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
    """, po_df.values.tolist())
    
    print(f"{Fore.GREEN}✓ Inserted {len(purchase_orders):,} purchase orders{Style.RESET_ALL}")
    
    # Now insert deliveries
    print(f"Inserting {len(deliveries):,} delivery records...")
    
    # Select only the columns needed for inbound_deliveries
    delivery_columns = [
        'delivery_id', 'po_id', 'productId', 'sku', 'quantity', 'unit_cost',
        'expected_delivery_date', 'actual_delivery_date', 'expiration_date',
        'batch_number', 'status', 'created_at'
    ]
    
    delivery_df = df[delivery_columns]
    
    # Insert in batches
    batch_size = 1000
    for i in range(0, len(delivery_df), batch_size):
        batch = delivery_df.iloc[i:i+batch_size]
        
        conn.executemany("""
            INSERT INTO inbound_deliveries (
                delivery_id, po_id, productId, sku, quantity, unit_cost,
                expected_delivery_date, actual_delivery_date, expiration_date,
                batch_number, status, created_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
        """, batch.values.tolist())
    
    print(f"{Fore.GREEN}✓ Inserted {len(deliveries):,} delivery records{Style.RESET_ALL}")

def generate_performance_report(conn, delivery_stats):
    """Generate supplier performance report"""
    print_header("Supplier Performance Report")
    
    # Overall stats
    late_rate = (delivery_stats['late'] / delivery_stats['total'] * 100) if delivery_stats['total'] > 0 else 0
    print(f"\nOverall Performance:")
    print(f"  Total deliveries: {delivery_stats['total']:,}")
    print(f"  Late deliveries: {delivery_stats['late']:,} ({late_rate:.1f}%)")
    
    # By supplier
    print(f"\nSupplier Performance Ranking:")
    supplier_stats = []
    
    for sid, stats in delivery_stats['by_supplier'].items():
        late_pct = (stats['late'] / stats['total'] * 100) if stats['total'] > 0 else 0
        supplier_stats.append({
            'id': sid,
            'name': stats['name'],
            'total': stats['total'],
            'late': stats['late'],
            'late_pct': late_pct,
            'on_time_pct': 100 - late_pct
        })
    
    # Sort by on-time percentage
    supplier_stats.sort(key=lambda x: x['on_time_pct'], reverse=True)
    
    print(f"\n{'Rank':<5} {'Supplier':<20} {'Deliveries':<12} {'On-Time %':<10} {'Late':<10}")
    print("-" * 60)
    
    for i, supplier in enumerate(supplier_stats, 1):
        status = f"{Fore.GREEN}✓{Style.RESET_ALL}" if supplier['on_time_pct'] >= 90 else \
                 f"{Fore.YELLOW}!{Style.RESET_ALL}" if supplier['on_time_pct'] >= 85 else \
                 f"{Fore.RED}✗{Style.RESET_ALL}"
        
        print(f"{i:<5} {supplier['name']:<20} {supplier['total']:<12,} "
              f"{supplier['on_time_pct']:<10.1f} {supplier['late']:<10} {status}")
    
    # Check worst delivered products
    print_header("Most Frequently Late Products")
    
    late_products = conn.execute("""
        SELECT 
            p.name as product_name,
            COUNT(*) as total_deliveries,
            COUNT(CASE WHEN id.actual_delivery_date > id.expected_delivery_date THEN 1 END) as late_deliveries,
            ROUND(100.0 * COUNT(CASE WHEN id.actual_delivery_date > id.expected_delivery_date THEN 1 END) / COUNT(*), 1) as late_pct
        FROM inbound_deliveries id
        JOIN products p ON id.productId = p.productId
        WHERE id.status = 'DELIVERED'
        GROUP BY p.name
        HAVING COUNT(*) >= 5  -- At least 5 deliveries
        ORDER BY late_pct DESC
        LIMIT 10
    """).fetchall()
    
    print(f"\n{'Product':<40} {'Deliveries':<12} {'Late %':<10}")
    print("-" * 65)
    
    for product, total, late, late_pct in late_products:
        print(f"{product[:40]:<40} {total:<12} {late_pct:<10.1f}%")

def main():
    """Main execution function"""
    print_header("Realistic Inbound Delivery Generation")
    print(f"Timestamp: {datetime.now()}")
    
    # Connect to database
    conn = duckdb.connect('../data/grocery_final.db')
    
    try:
        # No transaction for purge - commit immediately
        # 1. Purge existing deliveries
        purged_count = purge_existing_deliveries(conn)
        
        # Start transaction for inserts
        conn.begin()
        
        # 2. Generate historical deliveries (Jan 1 - July 14)
        historical_deliveries, historical_stats = generate_historical_deliveries(
            conn, '2025-01-01', '2025-07-15'
        )
        
        # 3. Generate future deliveries (July 15-18)
        future_deliveries = generate_future_deliveries(
            conn, datetime(2025, 7, 15).date(), 4
        )
        
        # 4. Insert all deliveries
        all_deliveries = historical_deliveries + future_deliveries
        insert_deliveries(conn, all_deliveries)
        
        # 5. Generate performance report
        generate_performance_report(conn, historical_stats)
        
        # Commit transaction
        conn.commit()
        print(f"\n{Fore.GREEN}✓ Successfully committed all changes{Style.RESET_ALL}")
        
        # Final summary
        print_header("Delivery Generation Summary")
        print(f"✓ Purged {purged_count:,} unrealistic deliveries")
        print(f"✓ Generated {len(historical_deliveries):,} historical deliveries")
        print(f"✓ Generated {len(future_deliveries):,} future deliveries")
        print(f"✓ Total new deliveries: {len(all_deliveries):,}")
        
        # Check new totals
        new_total = conn.execute("SELECT COUNT(*), SUM(quantity) FROM inbound_deliveries").fetchone()
        print(f"\nNew delivery totals:")
        print(f"  Records: {new_total[0]:,}")
        print(f"  Units: {new_total[1]:,}")
        
    except Exception as e:
        # Rollback on error
        conn.rollback()
        print(f"\n{Fore.RED}✗ Error occurred, rolling back: {str(e)}{Style.RESET_ALL}")
        raise
    
    finally:
        conn.close()

if __name__ == "__main__":
    main()