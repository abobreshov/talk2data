#!/usr/bin/env python3
"""
Generate comprehensive business reports from the database.
Creates multiple report types with visualizations.
"""

import duckdb
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from datetime import datetime, timedelta
import json
from colorama import init, Fore, Style

# Initialize colorama
init()

DATA_DIR = Path(__file__).parent.parent / 'data'
REPORTS_DIR = DATA_DIR / 'reports'
DB_PATH = DATA_DIR / 'grocery_final.db'

def print_header(text):
    """Print formatted header"""
    print(f"\n{Fore.CYAN}{'='*60}{Style.RESET_ALL}")
    print(f"{Fore.CYAN}{text}{Style.RESET_ALL}")
    print(f"{Fore.CYAN}{'='*60}{Style.RESET_ALL}")

def generate_sales_report(conn):
    """Generate sales performance report"""
    print("\n1. Generating Sales Report...")
    
    # Daily sales trend
    daily_sales = conn.execute("""
        SELECT 
            DATE_TRUNC('day', saleDate) as sale_date,
            COUNT(DISTINCT orderId) as num_orders,
            SUM(quantity) as units_sold,
            SUM(quantity * unitPrice) as revenue
        FROM sales
        WHERE saleDate >= CURRENT_DATE - INTERVAL '30 days'
        GROUP BY DATE_TRUNC('day', saleDate)
        ORDER BY sale_date
    """).fetchdf()
    
    # Top products by revenue
    top_products = conn.execute("""
        SELECT 
            p.name as product_name,
            p.category,
            SUM(s.quantity) as units_sold,
            SUM(s.quantity * s.unitPrice) as revenue
        FROM sales s
        JOIN products p ON s.productId = p.productId
        WHERE s.saleDate >= CURRENT_DATE - INTERVAL '30 days'
        GROUP BY p.name, p.category
        ORDER BY revenue DESC
        LIMIT 20
    """).fetchdf()
    
    # Category performance
    category_perf = conn.execute("""
        SELECT 
            p.category,
            COUNT(DISTINCT s.orderId) as num_orders,
            SUM(s.quantity) as units_sold,
            SUM(s.quantity * s.unitPrice) as revenue,
            AVG(s.unitPrice) as avg_price
        FROM sales s
        JOIN products p ON s.productId = p.productId
        WHERE s.saleDate >= CURRENT_DATE - INTERVAL '30 days'
        GROUP BY p.category
        ORDER BY revenue DESC
    """).fetchdf()
    
    # Create visualizations
    fig, axes = plt.subplots(2, 2, figsize=(15, 10))
    fig.suptitle('Sales Performance Report - Last 30 Days', fontsize=16)
    
    # Plot 1: Daily sales trend
    ax1 = axes[0, 0]
    ax1.plot(daily_sales['sale_date'], daily_sales['revenue'], marker='o')
    ax1.set_title('Daily Revenue Trend')
    ax1.set_xlabel('Date')
    ax1.set_ylabel('Revenue (£)')
    ax1.tick_params(axis='x', rotation=45)
    
    # Plot 2: Top products
    ax2 = axes[0, 1]
    top_10 = top_products.head(10)
    ax2.barh(range(len(top_10)), top_10['revenue'])
    ax2.set_yticks(range(len(top_10)))
    ax2.set_yticklabels([name[:30] for name in top_10['product_name']])
    ax2.set_xlabel('Revenue (£)')
    ax2.set_title('Top 10 Products by Revenue')
    
    # Plot 3: Category distribution
    ax3 = axes[1, 0]
    ax3.pie(category_perf['revenue'], labels=category_perf['category'], autopct='%1.1f%%')
    ax3.set_title('Revenue by Category')
    
    # Plot 4: Units vs Revenue
    ax4 = axes[1, 1]
    ax4.scatter(category_perf['units_sold'], category_perf['revenue'])
    for i, cat in enumerate(category_perf['category']):
        ax4.annotate(cat, (category_perf['units_sold'].iloc[i], category_perf['revenue'].iloc[i]))
    ax4.set_xlabel('Units Sold')
    ax4.set_ylabel('Revenue (£)')
    ax4.set_title('Units Sold vs Revenue by Category')
    
    plt.tight_layout()
    plt.savefig(REPORTS_DIR / 'sales_performance_report.png', dpi=300, bbox_inches='tight')
    
    # Save data
    report_data = {
        'generated_at': datetime.now().isoformat(),
        'period': 'Last 30 days',
        'total_revenue': float(daily_sales['revenue'].sum()),
        'total_units': int(daily_sales['units_sold'].sum()),
        'total_orders': int(daily_sales['num_orders'].sum()),
        'avg_daily_revenue': float(daily_sales['revenue'].mean()),
        'top_category': category_perf.iloc[0]['category'],
        'top_product': top_products.iloc[0]['product_name']
    }
    
    with open(REPORTS_DIR / 'sales_performance_summary.json', 'w') as f:
        json.dump(report_data, f, indent=2)
    
    print(f"  {Fore.GREEN}✓{Style.RESET_ALL} Sales report generated")

def generate_inventory_report(conn):
    """Generate inventory status report"""
    print("\n2. Generating Inventory Report...")
    
    # Current stock levels
    stock_summary = conn.execute("""
        SELECT 
            p.category,
            COUNT(DISTINCT s.productId) as num_products,
            SUM(s.quantity_in_stock) as total_units,
            SUM(s.quantity_in_stock * s.purchase_price) as inventory_value,
            SUM(CASE WHEN s.expiration_date <= CURRENT_DATE + INTERVAL '7 days' 
                THEN s.quantity_in_stock ELSE 0 END) as expiring_soon
        FROM stock s
        JOIN products p ON s.productId = p.productId
        WHERE s.stock_status = 'AVAILABLE'
        GROUP BY p.category
    """).fetchdf()
    
    # Products with low stock
    low_stock = conn.execute("""
        SELECT 
            p.name as product_name,
            p.category,
            COALESCE(st.total_stock, 0) as current_stock,
            COALESCE(f.avg_daily_demand, 0) as avg_daily_demand,
            CASE 
                WHEN f.avg_daily_demand > 0 
                THEN st.total_stock / f.avg_daily_demand 
                ELSE NULL 
            END as days_of_stock
        FROM products p
        LEFT JOIN (
            SELECT productId, SUM(quantity_in_stock) as total_stock
            FROM stock
            WHERE stock_status = 'AVAILABLE'
            GROUP BY productId
        ) st ON p.productId = st.productId
        LEFT JOIN (
            SELECT productId, AVG(predicted_quantity) as avg_daily_demand
            FROM latest_forecasts
            WHERE target_date <= CURRENT_DATE + INTERVAL '7 days'
            GROUP BY productId
        ) f ON p.productId = f.productId
        WHERE st.total_stock < 50 OR (f.avg_daily_demand > 0 AND st.total_stock / f.avg_daily_demand < 3)
        ORDER BY days_of_stock NULLS FIRST
        LIMIT 20
    """).fetchdf()
    
    # Expiring products
    expiring = conn.execute("""
        SELECT 
            p.name as product_name,
            s.batch_number,
            s.quantity_in_stock,
            s.expiration_date,
            s.expiration_date - CURRENT_DATE as days_until_expiry
        FROM stock s
        JOIN products p ON s.productId = p.productId
        WHERE s.stock_status = 'AVAILABLE'
        AND s.expiration_date <= CURRENT_DATE + INTERVAL '7 days'
        ORDER BY s.expiration_date
        LIMIT 20
    """).fetchdf()
    
    # Create visualizations
    fig, axes = plt.subplots(2, 2, figsize=(15, 10))
    fig.suptitle('Inventory Status Report', fontsize=16)
    
    # Plot 1: Stock by category
    ax1 = axes[0, 0]
    ax1.bar(stock_summary['category'], stock_summary['total_units'])
    ax1.set_xlabel('Category')
    ax1.set_ylabel('Units in Stock')
    ax1.set_title('Current Stock Levels by Category')
    ax1.tick_params(axis='x', rotation=45)
    
    # Plot 2: Inventory value
    ax2 = axes[0, 1]
    ax2.pie(stock_summary['inventory_value'], labels=stock_summary['category'], autopct='%1.1f%%')
    ax2.set_title('Inventory Value Distribution')
    
    # Plot 3: Expiring soon
    ax3 = axes[1, 0]
    ax3.barh(stock_summary['category'], stock_summary['expiring_soon'], color='orange')
    ax3.set_xlabel('Units Expiring Soon')
    ax3.set_title('Products Expiring in Next 7 Days')
    
    # Plot 4: Low stock alert
    ax4 = axes[1, 1]
    if not low_stock.empty:
        low_stock_sample = low_stock.head(10)
        ax4.barh(range(len(low_stock_sample)), low_stock_sample['current_stock'], color='red')
        ax4.set_yticks(range(len(low_stock_sample)))
        ax4.set_yticklabels([name[:25] for name in low_stock_sample['product_name']])
        ax4.set_xlabel('Current Stock')
        ax4.set_title('Top 10 Low Stock Products')
    
    plt.tight_layout()
    plt.savefig(REPORTS_DIR / 'inventory_status_report.png', dpi=300, bbox_inches='tight')
    
    # Save data
    report_data = {
        'generated_at': datetime.now().isoformat(),
        'total_inventory_value': float(stock_summary['inventory_value'].sum()),
        'total_units_in_stock': int(stock_summary['total_units'].sum()),
        'units_expiring_soon': int(stock_summary['expiring_soon'].sum()),
        'products_with_low_stock': len(low_stock),
        'products_expiring_soon': len(expiring)
    }
    
    with open(REPORTS_DIR / 'inventory_status_summary.json', 'w') as f:
        json.dump(report_data, f, indent=2)
    
    print(f"  {Fore.GREEN}✓{Style.RESET_ALL} Inventory report generated")

def generate_customer_report(conn):
    """Generate customer analytics report"""
    print("\n3. Generating Customer Report...")
    
    # Customer segments
    segments = conn.execute("""
        SELECT 
            CASE 
                WHEN lifetime_value = 0 THEN 'New'
                WHEN lifetime_value < 500 THEN 'Low Value'
                WHEN lifetime_value < 2000 THEN 'Medium Value'
                ELSE 'High Value'
            END as segment,
            COUNT(*) as customer_count,
            AVG(lifetime_value) as avg_lifetime_value,
            AVG(total_orders) as avg_orders,
            AVG(avg_order_value) as avg_basket_size
        FROM customer_analytics
        GROUP BY segment
        ORDER BY avg_lifetime_value DESC
    """).fetchdf()
    
    # Top customers
    top_customers = conn.execute("""
        SELECT 
            first_name || ' ' || last_name as customer_name,
            city,
            total_orders,
            delivered_orders,
            lifetime_value,
            avg_order_value
        FROM customer_analytics
        WHERE lifetime_value > 0
        ORDER BY lifetime_value DESC
        LIMIT 20
    """).fetchdf()
    
    # City distribution
    city_dist = conn.execute("""
        SELECT 
            city,
            COUNT(*) as customer_count,
            SUM(lifetime_value) as total_value,
            AVG(lifetime_value) as avg_value
        FROM customer_analytics
        GROUP BY city
        ORDER BY customer_count DESC
        LIMIT 10
    """).fetchdf()
    
    # Save summary
    report_data = {
        'generated_at': datetime.now().isoformat(),
        'total_customers': int(segments['customer_count'].sum()),
        'total_lifetime_value': float(top_customers['lifetime_value'].sum()),
        'avg_customer_value': float(top_customers['lifetime_value'].mean()),
        'segments': segments.to_dict('records')
    }
    
    with open(REPORTS_DIR / 'customer_analytics_summary.json', 'w') as f:
        json.dump(report_data, f, indent=2)
    
    print(f"  {Fore.GREEN}✓{Style.RESET_ALL} Customer report generated")

def generate_reports():
    """Generate all business reports"""
    
    print_header("Business Report Generation")
    print(f"Timestamp: {datetime.now()}")
    
    # Create reports directory
    REPORTS_DIR.mkdir(exist_ok=True)
    
    # Connect to database
    conn = duckdb.connect(str(DB_PATH))
    
    try:
        # Generate each report type
        generate_sales_report(conn)
        generate_inventory_report(conn)
        generate_customer_report(conn)
        
        # Create index file
        print("\n4. Creating report index...")
        index_content = f"""# Business Reports
Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## Available Reports

### 1. Sales Performance Report
- **File**: sales_performance_report.png
- **Summary**: sales_performance_summary.json
- **Period**: Last 30 days
- **Key Metrics**: Revenue, units sold, top products, category performance

### 2. Inventory Status Report
- **File**: inventory_status_report.png
- **Summary**: inventory_status_summary.json
- **Key Metrics**: Stock levels, expiring products, low stock alerts

### 3. Customer Analytics Report
- **Summary**: customer_analytics_summary.json
- **Key Metrics**: Customer segments, lifetime value, top customers

## How to Use

1. View PNG files for visual reports
2. Check JSON files for detailed metrics
3. All data is current as of report generation time
"""
        
        with open(REPORTS_DIR / 'README.md', 'w') as f:
            f.write(index_content)
        
        print(f"  {Fore.GREEN}✓{Style.RESET_ALL} Report index created")
        
    finally:
        conn.close()
    
    print_header("Report Generation Complete")
    print(f"All reports saved to: {REPORTS_DIR}")

if __name__ == "__main__":
    generate_reports()