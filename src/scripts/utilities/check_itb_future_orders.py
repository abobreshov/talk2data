#!/usr/bin/env python3
"""Check if ITB uses FUTURE orders"""

import duckdb

# Connect to database
conn = duckdb.connect('../data/grocery_final.db')

# Check if live basket dates align with FUTURE orders
print('Comparing live basket dates with FUTURE orders:')
comparison = conn.execute('''
    WITH basket_dates AS (
        -- Extract date from session_id (format: SES_customerId_YYYYMMDD_hash)
        SELECT 
            CAST(
                SUBSTR(session_id, POSITION('_' IN SUBSTR(session_id, 5)) + 5, 4) || '-' ||
                SUBSTR(session_id, POSITION('_' IN SUBSTR(session_id, 5)) + 9, 2) || '-' ||
                SUBSTR(session_id, POSITION('_' IN SUBSTR(session_id, 5)) + 11, 2) 
                AS DATE
            ) as basket_date,
            COUNT(DISTINCT session_id) as sessions,
            COUNT(DISTINCT customerId) as customers,
            SUM(quantity) as total_quantity
        FROM live_basket
        GROUP BY 1
    ),
    future_dates AS (
        SELECT 
            DATE_TRUNC('day', orderDate)::DATE as order_date,
            COUNT(DISTINCT o.orderId) as orders,
            COUNT(DISTINCT customerId) as customers,
            SUM(oi.quantity) as total_quantity
        FROM orders o
        JOIN order_items oi ON o.orderId = oi.orderId
        WHERE o.orderStatus = 'FUTURE'
        GROUP BY 1
    )
    SELECT 
        COALESCE(b.basket_date, f.order_date) as date,
        COALESCE(b.sessions, 0) as basket_sessions,
        COALESCE(b.customers, 0) as basket_customers,
        COALESCE(b.total_quantity, 0) as basket_qty,
        COALESCE(f.orders, 0) as future_orders,
        COALESCE(f.customers, 0) as future_customers,
        COALESCE(f.total_quantity, 0) as future_qty
    FROM basket_dates b
    FULL OUTER JOIN future_dates f ON b.basket_date = f.order_date
    WHERE COALESCE(b.basket_date, f.order_date) >= CURRENT_DATE
    ORDER BY 1
''').fetchall()

print('Date       | Basket Sessions | Basket Qty | Future Orders | Future Qty')
print('-' * 70)
for row in comparison:
    print(f'{row[0]} | {row[1]:15} | {row[3]:10} | {row[4]:13} | {row[6]:10}')

# Check order status distribution
print('\nOrder status distribution by date:')
status_dist = conn.execute('''
    SELECT 
        DATE_TRUNC('day', orderDate)::DATE as order_date,
        orderStatus,
        COUNT(*) as count
    FROM orders
    WHERE orderDate >= CURRENT_DATE
    GROUP BY 1, 2
    ORDER BY 1, 2
''').fetchall()

current_date = None
for row in status_dist:
    if row[0] != current_date:
        print(f'\n{row[0]}:')
        current_date = row[0]
    print(f'  {row[1]}: {row[2]}')

conn.close()