# Task 16: Sales Table Generation

## Objective
Generate a sales table from completed orders, where each sale record represents an item sold on the delivery date.

## Business Logic

1. **Sales occur on delivery date**, not order date
2. Only **DELIVERED** orders generate sales
3. **CANCELLED** orders do not create sales records
4. Each order item becomes one sales record
5. Sales preserve the exact quantity and price from order_items

## Implementation Steps

### 1. Create Sales Table Schema
```python
import duckdb
import pandas as pd
import uuid
from datetime import datetime

# Connect to orders database
conn = duckdb.connect('src/data/orders.duckdb')

# Create sales table
conn.execute("""
    CREATE TABLE IF NOT EXISTS sales (
        saleId VARCHAR PRIMARY KEY,
        orderId VARCHAR,
        productId VARCHAR,
        saleDate TIMESTAMP,
        quantity INTEGER,
        price DECIMAL(10,2),
        totalAmount DECIMAL(10,2)
    )
""")

# Create indexes for performance
conn.execute("CREATE INDEX IF NOT EXISTS idx_sales_date ON sales(saleDate)")
conn.execute("CREATE INDEX IF NOT EXISTS idx_sales_product ON sales(productId)")
conn.execute("CREATE INDEX IF NOT EXISTS idx_sales_order ON sales(orderId)")
```

### 2. Generate Sales from Delivered Orders
```python
def generate_sales_from_orders(conn):
    """Generate sales records from delivered orders"""
    
    # Get all delivered orders with their items
    delivered_items = conn.execute("""
        SELECT 
            o.orderId,
            o.deliveryDate as saleDate,
            oi.productId,
            oi.quantity,
            oi.price
        FROM orders o
        JOIN order_items oi ON o.orderId = oi.orderId
        WHERE o.orderStatus = 'DELIVERED'
        ORDER BY o.deliveryDate, o.orderId
    """).fetchdf()
    
    print(f"Found {len(delivered_items)} items from delivered orders")
    
    # Generate sale IDs and calculate totals
    delivered_items['saleId'] = ['SALE_' + str(uuid.uuid4())[:8] for _ in range(len(delivered_items))]
    delivered_items['totalAmount'] = delivered_items['quantity'] * delivered_items['price']
    
    # Insert into sales table
    conn.execute("DELETE FROM sales")  # Clear existing sales
    conn.execute("INSERT INTO sales SELECT * FROM delivered_items")
    
    return delivered_items
```

### 3. Validate Sales Generation
```python
def validate_sales(conn):
    """Validate sales table integrity"""
    
    # Check 1: Sales count matches delivered order items
    sales_count = conn.execute("SELECT COUNT(*) FROM sales").fetchone()[0]
    delivered_items_count = conn.execute("""
        SELECT COUNT(*) 
        FROM orders o
        JOIN order_items oi ON o.orderId = oi.orderId
        WHERE o.orderStatus = 'DELIVERED'
    """).fetchone()[0]
    
    print(f"Sales records: {sales_count}")
    print(f"Delivered order items: {delivered_items_count}")
    print(f"Match: {'✓' if sales_count == delivered_items_count else '✗'}")
    
    # Check 2: No sales for cancelled orders
    cancelled_sales = conn.execute("""
        SELECT COUNT(*) 
        FROM sales s
        JOIN orders o ON s.orderId = o.orderId
        WHERE o.orderStatus = 'CANCELLED'
    """).fetchone()[0]
    
    print(f"\nSales for cancelled orders: {cancelled_sales}")
    print(f"Correct (should be 0): {'✓' if cancelled_sales == 0 else '✗'}")
    
    # Check 3: Sales dates match delivery dates
    date_mismatches = conn.execute("""
        SELECT COUNT(*)
        FROM sales s
        JOIN orders o ON s.orderId = o.orderId
        WHERE DATE(s.saleDate) != DATE(o.deliveryDate)
    """).fetchone()[0]
    
    print(f"\nSales with incorrect dates: {date_mismatches}")
    print(f"Correct (should be 0): {'✓' if date_mismatches == 0 else '✗'}")
```

### 4. Sales Analytics
```python
def analyze_sales(conn):
    """Generate sales analytics"""
    
    # Daily sales summary
    daily_sales = conn.execute("""
        SELECT 
            DATE(saleDate) as date,
            COUNT(DISTINCT orderId) as orders,
            COUNT(*) as items_sold,
            SUM(quantity) as units_sold,
            SUM(totalAmount) as revenue
        FROM sales
        GROUP BY DATE(saleDate)
        ORDER BY date DESC
        LIMIT 30
    """).fetchdf()
    
    print("\nRecent daily sales:")
    print(daily_sales.head())
    
    # Top selling products
    top_products = conn.execute("""
        SELECT 
            productId,
            COUNT(*) as times_sold,
            SUM(quantity) as units_sold,
            SUM(totalAmount) as revenue
        FROM sales
        GROUP BY productId
        ORDER BY revenue DESC
        LIMIT 20
    """).fetchdf()
    
    print("\nTop selling products:")
    print(top_products.head())
    
    # Sales trend over time
    monthly_trend = conn.execute("""
        SELECT 
            DATE_TRUNC('month', saleDate) as month,
            COUNT(DISTINCT orderId) as orders,
            SUM(totalAmount) as revenue,
            AVG(totalAmount/quantity) as avg_price
        FROM sales
        GROUP BY month
        ORDER BY month
    """).fetchdf()
    
    return daily_sales, top_products, monthly_trend
```

### 5. Create Sales Visualization
```python
import matplotlib.pyplot as plt

def visualize_sales_trends(monthly_trend, daily_sales_recent):
    """Create sales trend visualizations"""
    
    fig, axes = plt.subplots(2, 1, figsize=(12, 8))
    
    # Monthly revenue trend
    ax1 = axes[0]
    ax1.plot(monthly_trend['month'], monthly_trend['revenue'], marker='o')
    ax1.set_title('Monthly Sales Revenue Trend')
    ax1.set_xlabel('Month')
    ax1.set_ylabel('Revenue (£)')
    ax1.grid(True, alpha=0.3)
    
    # Daily sales (last 30 days)
    ax2 = axes[1]
    ax2.bar(range(len(daily_sales_recent)), daily_sales_recent['revenue'])
    ax2.set_title('Daily Sales Revenue (Last 30 Days)')
    ax2.set_xlabel('Days Ago')
    ax2.set_ylabel('Revenue (£)')
    ax2.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig('src/data/sales_trends.png')
    plt.close()
```

## Expected Output

1. **Sales Table**:
   - One record per order item delivered
   - Accurate dates (delivery date = sale date)
   - Preserves quantity and pricing

2. **Validation Report**:
   - Sales count matches delivered items
   - No sales for cancelled orders
   - Dates align correctly

3. **Analytics**:
   - Daily/monthly sales trends
   - Top selling products
   - Revenue patterns

## Success Criteria

- [ ] All delivered order items have corresponding sales
- [ ] No sales exist for cancelled orders
- [ ] Sales dates equal delivery dates
- [ ] Sales can be aggregated for reporting
- [ ] Performance is acceptable for 2 years of data