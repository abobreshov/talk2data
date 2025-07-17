#!/usr/bin/env python3
"""
Generate sales for orders that were recently marked as DELIVERED (July 10-14)
"""

import duckdb
import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path
import uuid
from tqdm import tqdm

# Paths
data_dir = Path(__file__).parent.parent / 'data'
db_path = data_dir / 'grocery_final.db'

print("=== Generating Sales for July 10-14 Delivered Orders ===")

# Connect to the final database
conn = duckdb.connect(str(db_path))

# Check current sales status
current_status = conn.execute("""
    SELECT 
        MIN(CAST(saleDate AS DATE)) as first_sale,
        MAX(CAST(saleDate AS DATE)) as last_sale,
        COUNT(*) as total_sales
    FROM sales
""").fetchone()

print(f"\nCurrent sales status:")
print(f"  First sale: {current_status[0]}")
print(f"  Last sale: {current_status[1]}")
print(f"  Total sales: {current_status[2]:,}")

# Get delivered orders for July 10-14 that don't have sales
unprocessed_orders = conn.execute("""
    SELECT DISTINCT 
        o.orderId, 
        CAST(o.deliveryDate AS DATE) as delivery_date,
        o.customerId
    FROM orders o
    WHERE o.orderStatus = 'DELIVERED'
    AND CAST(o.deliveryDate AS DATE) >= '2025-07-10'
    AND CAST(o.deliveryDate AS DATE) <= '2025-07-14'
    AND NOT EXISTS (
        SELECT 1 FROM sales s 
        WHERE s.orderId = o.orderId
    )
    ORDER BY o.deliveryDate
""").fetchdf()

print(f"\nFound {len(unprocessed_orders)} delivered orders without sales for July 10-14")

if len(unprocessed_orders) == 0:
    print("No unprocessed orders found. Checking if sales already exist...")
    
    # Check if we have any sales for July 10-14
    july_sales = conn.execute("""
        SELECT 
            CAST(saleDate AS DATE) as sale_date,
            COUNT(*) as count
        FROM sales
        WHERE CAST(saleDate AS DATE) >= '2025-07-10'
        AND CAST(saleDate AS DATE) <= '2025-07-14'
        GROUP BY CAST(saleDate AS DATE)
        ORDER BY sale_date
    """).fetchdf()
    
    if len(july_sales) > 0:
        print("\nSales already exist for July 10-14:")
        print(july_sales)
    
    conn.close()
    exit()

# Get order items for these orders
order_ids_str = "', '".join(unprocessed_orders['orderId'].tolist())
order_items_df = conn.execute(f"""
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
    WHERE oi.orderId IN ('{order_ids_str}')
""").fetchdf()

print(f"Found {len(order_items_df)} order items to process")

# Load existing sales to continue numbering
max_sale_id = conn.execute("SELECT MAX(saleId) FROM sales").fetchone()[0]
print(f"Last sale ID: {max_sale_id}")

# Generate sales records
sales_records = []
print("\nGenerating sales records...")

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
print(f"\nGenerated {len(new_sales_df)} new sales records")

# Summary by date
summary = new_sales_df.groupby(pd.to_datetime(new_sales_df['saleDate']).dt.date).size()
print("\nNew sales by date:")
for date, count in summary.items():
    print(f"  {date}: {count:,} sales")

# Insert into sales table
print("\nInserting new sales into database...")
conn.execute("INSERT INTO sales SELECT * FROM new_sales_df")

# Verify the update
print("\n=== Verification ===")
total_sales = conn.execute("SELECT COUNT(*) FROM sales").fetchone()[0]
print(f"Total sales records: {total_sales:,}")

# Check sales by date for July 10-14
recent_sales = conn.execute("""
    SELECT 
        CAST(saleDate AS DATE) as sale_date,
        COUNT(*) as sales_count,
        COUNT(DISTINCT orderId) as order_count,
        COUNT(DISTINCT customerId) as customers
    FROM sales
    WHERE CAST(saleDate AS DATE) >= '2025-07-10'
    GROUP BY CAST(saleDate AS DATE)
    ORDER BY sale_date
""").fetchdf()

print("\nSales for July 10-14:")
print(recent_sales)

# Export all sales to CSV to keep it in sync
print("\nExporting updated sales to CSV...")
all_sales = conn.execute("""
    SELECT * FROM sales 
    ORDER BY saleDate, saleId
""").fetchdf()
all_sales.to_csv(data_dir / 'sales.csv', index=False)
print(f"✓ Exported {len(all_sales):,} sales records to sales.csv")

# Update sales summary
new_summary = {
    'generation_timestamp': datetime.now().isoformat(),
    'total_sales_records': len(all_sales),
    'unique_products': all_sales['productId'].nunique(),
    'unique_customers': all_sales['customerId'].nunique(),
    'date_range': {
        'start': str(all_sales['saleDate'].min()[:10]),
        'end': str(all_sales['saleDate'].max()[:10])
    },
    'new_records_added': len(new_sales_df),
    'july_10_14_sales': len(new_sales_df)
}

import json
with open(data_dir / 'sales_generation_summary_updated.json', 'w') as f:
    json.dump(new_summary, f, indent=2)

conn.close()
print("\n✓ Sales generation complete!")
print(f"✓ Added {len(new_sales_df):,} sales for July 10-14")