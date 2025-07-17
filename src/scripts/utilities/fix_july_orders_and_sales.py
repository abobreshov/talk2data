#!/usr/bin/env python3
"""
Fix July 10-14 orders by updating to DELIVERED and generating sales
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import uuid
from tqdm import tqdm

# Paths
data_dir = Path(__file__).parent.parent / 'data'

print("=== Fixing July 10-14 Orders and Generating Sales ===")

# 1. Load and update orders
print("\n1. Loading and updating orders...")
orders_df = pd.read_csv(data_dir / 'orders.csv')
print(f"  Total orders: {len(orders_df)}")

# Convert deliveryDate to datetime for proper comparison
orders_df['deliveryDate_dt'] = pd.to_datetime(orders_df['deliveryDate'])

# Find July 10-14 orders
july_mask = (orders_df['deliveryDate_dt'].dt.date >= pd.Timestamp('2025-07-10').date()) & \
            (orders_df['deliveryDate_dt'].dt.date <= pd.Timestamp('2025-07-14').date())

july_orders = orders_df[july_mask].copy()
print(f"  July 10-14 orders: {len(july_orders)}")
print(f"  Current status: {july_orders['orderStatus'].value_counts().to_dict()}")

# Update status to DELIVERED
orders_df.loc[july_mask, 'orderStatus'] = 'DELIVERED'

# Save updated orders
orders_df.drop('deliveryDate_dt', axis=1, inplace=True)
orders_df.to_csv(data_dir / 'orders.csv', index=False)
print(f"  ✓ Updated {july_mask.sum()} orders to DELIVERED")

# 2. Generate sales for these orders
print("\n2. Loading order items...")
order_items_df = pd.read_csv(data_dir / 'order_items.csv', dtype={'created_at': str})

# Get order items for July 10-14 orders
july_order_ids = july_orders['orderId'].tolist()
july_items = order_items_df[order_items_df['orderId'].isin(july_order_ids)].copy()
print(f"  Order items to process: {len(july_items)}")

# Merge with order info to get delivery dates
july_items = july_items.merge(
    july_orders[['orderId', 'customerId', 'deliveryDate']], 
    on='orderId', 
    how='left'
)

# 3. Load existing sales
print("\n3. Loading existing sales...")
sales_df = pd.read_csv(data_dir / 'sales.csv')
print(f"  Current sales records: {len(sales_df)}")

# 4. Generate new sales records
print("\n4. Generating new sales records...")
new_sales = []

for _, item in tqdm(july_items.iterrows(), total=len(july_items), desc="Creating sales"):
    # Ensure productId is 18 characters
    product_id = str(item['productId']).zfill(18)
    
    # Create individual sales records for each unit
    for _ in range(int(item['quantity'])):
        sale_id = f"SALE{uuid.uuid4().hex[:12].upper()}"
        new_sales.append({
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

new_sales_df = pd.DataFrame(new_sales)
print(f"  Generated {len(new_sales_df)} new sales records")

# Summary by date
if len(new_sales_df) > 0:
    summary = new_sales_df.groupby(pd.to_datetime(new_sales_df['saleDate']).dt.date).size()
    print("\n  New sales by date:")
    for sale_date, count in summary.items():
        print(f"    {sale_date}: {count:,} sales")

# 5. Append to existing sales
print("\n5. Appending to sales data...")
updated_sales_df = pd.concat([sales_df, new_sales_df], ignore_index=True)
updated_sales_df.to_csv(data_dir / 'sales.csv', index=False)
print(f"  ✓ Total sales records: {len(updated_sales_df)}")

# 6. Update sales summary
print("\n6. Updating sales summary...")
summary_data = {
    'generation_timestamp': datetime.now().isoformat(),
    'total_sales_records': len(updated_sales_df),
    'unique_products': updated_sales_df['productId'].nunique(),
    'unique_customers': updated_sales_df['customerId'].nunique(),
    'date_range': {
        'start': str(pd.to_datetime(updated_sales_df['saleDate']).min().date()),
        'end': str(pd.to_datetime(updated_sales_df['saleDate']).max().date())
    },
    'july_10_14_sales': len(new_sales_df),
    'total_revenue': float(updated_sales_df['unitPrice'].sum())
}

import json
with open(data_dir / 'sales_generation_summary.json', 'w') as f:
    json.dump(summary_data, f, indent=2)

print("\n✓ Complete! July 10-14 orders updated and sales generated.")
print(f"  - Updated {len(july_orders)} orders to DELIVERED")
print(f"  - Generated {len(new_sales_df)} new sales records")
print(f"  - Sales now cover: {summary_data['date_range']['start']} to {summary_data['date_range']['end']}")