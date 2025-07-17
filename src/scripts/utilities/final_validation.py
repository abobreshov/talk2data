#!/usr/bin/env python3
"""Final validation of all requirements"""

import duckdb
from datetime import datetime
import os

# Connect to database
conn = duckdb.connect('../data/grocery_final.db')

print('=== FINAL DATA VALIDATION ===')

# 1. Check product distribution in sales
print('\n1. Product Distribution in Recent Sales:')
top_products = conn.execute('''
    SELECT 
        p.name,
        COUNT(DISTINCT s.saleDate) as days_sold,
        SUM(s.quantity) as total_quantity,
        SUM(s.quantity * s.unitPrice / 100.0) as total_revenue,
        (SUM(s.quantity) * 100.0 / (SELECT SUM(quantity) FROM sales WHERE saleDate >= CURRENT_DATE - INTERVAL '30 days')) as pct_of_sales
    FROM sales s
    JOIN products p ON s.productId = p.productId
    WHERE s.saleDate >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY p.name
    ORDER BY total_quantity DESC
    LIMIT 10
''').fetchall()

print('Top 10 Products by Sales (Last 30 Days):')
print(f'{"Product":40} | {"Days":>5} | {"Qty":>8} | {"Revenue":>10} | {"% Sales":>8}')
print('-' * 85)
for row in top_products:
    print(f'{row[0][:40]:40} | {row[1]:5} | {row[2]:8} | {row[3]:10.2f} | {row[4]:7.2f}%')

# 2. Check sales data coverage
print('\n2. Sales Data Coverage:')
sales_coverage = conn.execute('''
    SELECT 
        MIN(DATE_TRUNC('day', saleDate)::DATE) as first_sale,
        MAX(DATE_TRUNC('day', saleDate)::DATE) as last_sale,
        COUNT(DISTINCT DATE_TRUNC('day', saleDate)::DATE) as days_with_sales
    FROM sales
''').fetchone()
print(f'  First sale: {sales_coverage[0]}')
print(f'  Last sale: {sales_coverage[1]}')
print(f'  Days with sales: {sales_coverage[2]}')

# 3. Check order status by date
print('\n3. Order Status by Date (July 14-20):')
order_status = conn.execute('''
    SELECT 
        DATE_TRUNC('day', orderDate)::DATE as order_date,
        orderStatus,
        COUNT(*) as count
    FROM orders
    WHERE orderDate >= '2025-07-14' AND orderDate <= '2025-07-20'
    GROUP BY 1, 2
    ORDER BY 1, 2
''').fetchall()

current_date = None
for row in order_status:
    if row[0] != current_date:
        print(f'\n{row[0]}:')
        current_date = row[0]
    print(f'  {row[1]}: {row[2]}')

# 4. Check forecast coverage
print('\n4. Forecast Coverage:')
forecast_coverage = conn.execute('''
    SELECT 
        MIN(target_date) as first_forecast,
        MAX(target_date) as last_forecast,
        COUNT(DISTINCT productId) as products_forecasted,
        COUNT(*) as total_forecasts
    FROM forecasts
    WHERE forecast_date = CURRENT_DATE
''').fetchone()
print(f'  Forecast range: {forecast_coverage[0]} to {forecast_coverage[1]}')
print(f'  Products forecasted: {forecast_coverage[2]}')
print(f'  Total forecast records: {forecast_coverage[3]}')

# 5. Check supply chain tables
print('\n5. Supply Chain Tables:')
supply_tables = [
    ('suppliers', 'SELECT COUNT(*) FROM suppliers'),
    ('supplier_schedules', 'SELECT COUNT(*) FROM supplier_schedules'),
    ('product_ordering_calendar', 'SELECT COUNT(*) FROM product_ordering_calendar'),
    ('purchase_orders', 'SELECT COUNT(*) FROM purchase_orders'),
    ('inbound_deliveries', 'SELECT COUNT(*) FROM inbound_deliveries'),
    ('stock', 'SELECT COUNT(*) FROM stock'),
    ('live_basket', 'SELECT COUNT(*) FROM live_basket')
]

for table_name, query in supply_tables:
    count = conn.execute(query).fetchone()[0]
    print(f'  {table_name}: {count:,} records')

# 6. Check Parquet export
print('\n6. Parquet Export Status:')
parquet_dir = '../data/parquet_export'
if os.path.exists(parquet_dir):
    parquet_files = [f for f in os.listdir(parquet_dir) if f.endswith('.parquet')]
    print(f'  Files exported: {len(parquet_files)}')
    total_size = sum(os.path.getsize(os.path.join(parquet_dir, f)) for f in parquet_files) / (1024*1024)
    print(f'  Total size: {total_size:.2f} MB')
else:
    print('  Parquet export directory not found')

conn.close()

print('\n✓ All validations complete!')