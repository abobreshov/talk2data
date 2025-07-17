#!/usr/bin/env python3
"""
Generate sales for orders that were recently marked as DELIVERED
"""

import duckdb
import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path
import uuid
from tqdm import tqdm

# Connect to databases
orders_db = duckdb.connect('orders.duckdb')
data_dir = Path(__file__).parent.parent / 'data'

print("=== Generating Sales for Recently Delivered Orders ===")

# Get orders that don't have sales yet
unprocessed_orders = orders_db.execute("""
    SELECT DISTINCT o.orderId, o.deliveryDate::DATE as delivery_date
    FROM orders o
    WHERE o.orderStatus = 'DELIVERED'
    AND o.deliveryDate::DATE >= '2025-07-10'
    AND o.deliveryDate::DATE <= '2025-07-14'
    AND NOT EXISTS (
        SELECT 1 FROM sales s 
        WHERE s.orderId = o.orderId
    )
    ORDER BY o.deliveryDate
""").fetchall()

print(f"\nFound {len(unprocessed_orders)} orders without sales for July 10-14")

if len(unprocessed_orders) == 0:
    print("No unprocessed orders found. Exiting.")
    exit()

# Get order items for these orders
order_ids = [order[0] for order in unprocessed_orders]
order_items_query = f"""
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
    WHERE oi.orderId IN ({','.join([f"'{oid}'" for oid in order_ids])})
"""

order_items_df = orders_db.execute(order_items_query).fetchdf()
print(f"Found {len(order_items_df)} order items to process")

# Generate sales records
sales_records = []
print("\nGenerating sales records...")

for _, item in tqdm(order_items_df.iterrows(), total=len(order_items_df), desc="Creating sales"):
    # Create individual sales records for each unit
    for _ in range(int(item['quantity'])):
        sale_id = f"SALE{uuid.uuid4().hex[:12].upper()}"
        sales_records.append({
            'saleId': sale_id,
            'orderId': item['orderId'],
            'orderItemId': item['orderItemId'],
            'customerId': int(item['customerId']),
            'productId': item['productId'],
            'saleDate': item['deliveryDate'],
            'unitPrice': float(item['unitPrice']),
            'quantity': 1,
            'created_at': datetime.now()
        })

# Convert to DataFrame
new_sales_df = pd.DataFrame(sales_records)
print(f"\nGenerated {len(new_sales_df)} new sales records")

# Insert into sales table
print("\nInserting new sales into database...")
orders_db.execute("INSERT INTO sales SELECT * FROM new_sales_df")

# Verify the update
print("\n=== Verification ===")
total_sales = orders_db.execute("SELECT COUNT(*) FROM sales").fetchone()[0]
print(f"Total sales records: {total_sales:,}")

# Check sales by date for July 10-14
recent_sales = orders_db.execute("""
    SELECT 
        saleDate::DATE as sale_date,
        COUNT(*) as sales_count,
        COUNT(DISTINCT orderId) as order_count
    FROM sales
    WHERE saleDate::DATE >= '2025-07-10'
    GROUP BY saleDate::DATE
    ORDER BY saleDate::DATE
""").fetchall()

print("\nRecent sales (July 10 onwards):")
for row in recent_sales:
    print(f"  {row[0]}: {row[1]:,} sales from {row[2]:,} orders")

# Save updated sales to CSV
print("\nExporting updated sales to CSV...")
all_sales = orders_db.execute("SELECT * FROM sales ORDER BY saleDate, saleId").fetchdf()
all_sales.to_csv(data_dir / 'sales.csv', index=False)
print(f"✓ Exported {len(all_sales):,} sales records to sales.csv")

orders_db.close()
print("\n✓ Sales generation complete!")