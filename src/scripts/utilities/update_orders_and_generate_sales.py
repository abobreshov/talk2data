#!/usr/bin/env python3
"""
Update order statuses to DELIVERED for July 10-14 and generate sales
"""

import duckdb
import pandas as pd
import numpy as np
from datetime import datetime, date
from pathlib import Path
import uuid
from tqdm import tqdm

# Paths
data_dir = Path(__file__).parent.parent / 'data'
db_path = data_dir / 'grocery_final.db'

print("=== Updating Order Status and Generating Sales for July 10-14 ===")

# Connect to the final database
conn = duckdb.connect(str(db_path))

# 1. Update order statuses
print("\n1. Updating order statuses to DELIVERED...")

# Count orders to update
orders_to_update = conn.execute("""
    SELECT COUNT(*) as count
    FROM orders
    WHERE orderStatus IN ('PICKED', 'FUTURE')
    AND CAST(deliveryDate AS DATE) >= '2025-07-10'
    AND CAST(deliveryDate AS DATE) <= '2025-07-14'
""").fetchone()[0]

print(f"  Orders to update: {orders_to_update}")

# Update statuses - DuckDB doesn't allow direct updates with foreign keys
# So we need to recreate the data
orders_df = conn.execute("SELECT * FROM orders").fetchdf()

# Update the status in the dataframe
mask = (orders_df['orderStatus'].isin(['PICKED', 'FUTURE'])) & \
       (pd.to_datetime(orders_df['deliveryDate']).dt.date >= date(2025, 7, 10)) & \
       (pd.to_datetime(orders_df['deliveryDate']).dt.date <= date(2025, 7, 14))
       
orders_df.loc[mask, 'orderStatus'] = 'DELIVERED'

# Save the updated orders to CSV
orders_df.to_csv(data_dir / 'orders.csv', index=False)
print(f"  ✓ Updated {mask.sum()} orders to DELIVERED in CSV")

print(f"  ✓ Updated {orders_to_update} orders to DELIVERED")

# Reload the database to pick up the changes
print("\n  Reloading database with updated orders...")
conn.close()

# Run the create final database script
import subprocess
result = subprocess.run([
    '/home/abobreshov/miniconda3/envs/grocery_poc/bin/python',
    '11_create_final_database.py'
], capture_output=True, text=True)

if result.returncode != 0:
    print(f"Error reloading database: {result.stderr}")
    exit(1)

print("  ✓ Database reloaded")

# Reconnect
conn = duckdb.connect(str(db_path))

# 2. Get these orders and their items
print("\n2. Getting order items for sales generation...")

order_items_df = conn.execute("""
    SELECT 
        oi.orderId,
        oi.orderItemId,
        oi.productId,
        oi.quantity,
        oi.unitPrice,
        o.customerId,
        o.deliveryDate
    FROM order_items oi
    JOIN orders o ON oi.orderId = o.orderId
    WHERE o.orderStatus = 'DELIVERED'
    AND CAST(o.deliveryDate AS DATE) >= '2025-07-10'
    AND CAST(o.deliveryDate AS DATE) <= '2025-07-14'
    AND NOT EXISTS (
        SELECT 1 FROM sales s 
        WHERE s.orderId = o.orderId
    )
""").fetchdf()

print(f"  Found {len(order_items_df)} order items to process")

# 3. Generate sales records
sales_records = []
print("\n3. Generating sales records...")

for _, item in tqdm(order_items_df.iterrows(), total=len(order_items_df), desc="Creating sales"):
    # Ensure productId is 18 characters
    product_id = str(item['productId']).zfill(18)
    
    # Create individual sales records for each unit
    for _ in range(int(item['quantity'])):
        sale_id = f"SALE{uuid.uuid4().hex[:12].upper()}"
        sales_records.append({
            'saleId': sale_id,
            'orderId': item['orderId'],
            'orderItemId': item['orderItemId'],
            'customerId': int(item['customerId']),
            'productId': product_id,
            'saleDate': item['deliveryDate'],
            'unitPrice': float(item['unitPrice']),
            'quantity': 1,
            'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })

# Convert to DataFrame
new_sales_df = pd.DataFrame(sales_records)
print(f"\n  Generated {len(new_sales_df)} new sales records")

# Summary by date
if len(new_sales_df) > 0:
    summary = new_sales_df.groupby(pd.to_datetime(new_sales_df['saleDate']).dt.date).size()
    print("\n  New sales by date:")
    for sale_date, count in summary.items():
        print(f"    {sale_date}: {count:,} sales")

    # 4. Insert into sales table
    print("\n4. Inserting new sales into database...")
    conn.execute("INSERT INTO sales SELECT * FROM new_sales_df")
    print(f"  ✓ Inserted {len(new_sales_df)} sales records")

# 5. Export updated data to CSV
print("\n5. Exporting updated data to CSV files...")

# Export orders
all_orders = conn.execute("SELECT * FROM orders ORDER BY orderId").fetchdf()
all_orders.to_csv(data_dir / 'orders.csv', index=False)
print(f"  ✓ Exported {len(all_orders):,} orders to orders.csv")

# Export sales
all_sales = conn.execute("SELECT * FROM sales ORDER BY saleDate, saleId").fetchdf()
all_sales.to_csv(data_dir / 'sales.csv', index=False)
print(f"  ✓ Exported {len(all_sales):,} sales to sales.csv")

# 6. Verification
print("\n6. Final Verification:")

# Check order status distribution
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

print("\n  Order Status Summary:")
for _, row in order_status.iterrows():
    print(f"    {row['orderStatus']}: {row['count']:,} orders ({row['min_date']} to {row['max_date']})")

# Check sales date range
sales_summary = conn.execute("""
    SELECT 
        MIN(CAST(saleDate AS DATE)) as first_sale,
        MAX(CAST(saleDate AS DATE)) as last_sale,
        COUNT(*) as total_sales
    FROM sales
""").fetchone()

print(f"\n  Sales Summary:")
print(f"    Date range: {sales_summary[0]} to {sales_summary[1]}")
print(f"    Total sales: {sales_summary[2]:,}")

# Sales for July 10-14
recent_sales = conn.execute("""
    SELECT 
        CAST(saleDate AS DATE) as sale_date,
        COUNT(*) as sales_count,
        COUNT(DISTINCT orderId) as order_count,
        COUNT(DISTINCT customerId) as customers
    FROM sales
    WHERE CAST(saleDate AS DATE) >= '2025-07-10'
    AND CAST(saleDate AS DATE) <= '2025-07-14'
    GROUP BY CAST(saleDate AS DATE)
    ORDER BY sale_date
""").fetchdf()

print("\n  Sales for July 10-14:")
print(recent_sales)

conn.close()
print("\n✓ Update complete!")