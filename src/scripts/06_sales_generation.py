#!/usr/bin/env python3
"""
Task 16: Sales Table Generation
This script generates a sales table from delivered orders where each order item
creates a sale record on the delivery date.
"""

import os
import sys
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import json
from tqdm import tqdm

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

# Load configuration
config_path = Path(__file__).parent / "config.json"
with open(config_path, 'r') as f:
    config = json.load(f)

DATA_DIR = Path(config['data_dir'])

def check_prerequisites():
    """Check if required files from previous steps exist"""
    required_files = [
        (DATA_DIR / 'orders.csv', 'Orders (Task 15)'),
        (DATA_DIR / 'order_items.csv', 'Order items (Task 15)'),
    ]
    
    missing = []
    for file_path, description in required_files:
        if not file_path.exists():
            missing.append(f"  - {file_path.name} ({description})")
    
    if missing:
        print("✗ Missing required files from previous steps:")
        for item in missing:
            print(item)
        print("\nPlease run the previous tasks in order.")
        return False
    
    return True

def load_data():
    """Load orders and order items data"""
    print("=== Loading Data ===")
    
    # Load orders
    orders_df = pd.read_csv(DATA_DIR / 'orders.csv')
    orders_df['orderDate'] = pd.to_datetime(orders_df['orderDate'])
    orders_df['deliveryDate'] = pd.to_datetime(orders_df['deliveryDate'])
    print(f"✓ Loaded {len(orders_df)} orders")
    
    # Load order items
    order_items_df = pd.read_csv(DATA_DIR / 'order_items.csv')
    print(f"✓ Loaded {len(order_items_df)} order items")
    
    return orders_df, order_items_df

def generate_sales_table(orders_df, order_items_df):
    """Generate sales table from delivered orders"""
    print("\n=== Generating Sales Table ===")
    
    # Filter for delivered orders only
    delivered_orders = orders_df[orders_df['orderStatus'] == 'DELIVERED']
    print(f"Processing {len(delivered_orders)} delivered orders")
    
    # Join order items with delivered orders
    sales_data = order_items_df.merge(
        delivered_orders[['orderId', 'customerId', 'deliveryDate']],
        on='orderId',
        how='inner'
    )
    
    print(f"✓ Matched {len(sales_data)} order items from delivered orders")
    
    # Create sales records
    sales_records = []
    sale_id_counter = 1
    
    for _, item in tqdm(sales_data.iterrows(), total=len(sales_data), desc="Creating sales records"):
        # Create a sale record for each unit sold
        for unit in range(int(item['quantity'])):
            sale_record = {
                'saleId': f'SALE_{sale_id_counter:010d}',
                'orderId': item['orderId'],
                'orderItemId': item['orderItemId'],
                'customerId': item['customerId'],
                'productId': item['productId'],
                'saleDate': item['deliveryDate'],  # Sale recorded on delivery date
                'unitPrice': item['unitPrice'],
                'quantity': 1  # Each sale record represents 1 unit
            }
            sales_records.append(sale_record)
            sale_id_counter += 1
    
    # Create DataFrame
    sales_df = pd.DataFrame(sales_records)
    
    print(f"\n✓ Generated {len(sales_df)} sales records")
    
    return sales_df

def analyze_sales(sales_df):
    """Analyze the generated sales data"""
    print("\n=== Analyzing Sales Data ===")
    
    # Convert saleDate to datetime
    sales_df['saleDate'] = pd.to_datetime(sales_df['saleDate'])
    
    # Basic statistics
    print("\nSales Statistics:")
    print(f"  Total sales records: {len(sales_df):,}")
    print(f"  Unique products sold: {sales_df['productId'].nunique()}")
    print(f"  Unique customers: {sales_df['customerId'].nunique()}")
    print(f"  Date range: {sales_df['saleDate'].min().date()} to {sales_df['saleDate'].max().date()}")
    
    # Sales by date
    daily_sales = sales_df.groupby(sales_df['saleDate'].dt.date).agg({
        'saleId': 'count',
        'unitPrice': 'sum'
    }).rename(columns={'saleId': 'units_sold', 'unitPrice': 'revenue'})
    
    print(f"\nDaily Sales Summary:")
    print(f"  Average units per day: {daily_sales['units_sold'].mean():.1f}")
    print(f"  Average daily revenue: £{daily_sales['revenue'].mean():.2f}")
    print(f"  Peak daily units: {daily_sales['units_sold'].max():,}")
    print(f"  Peak daily revenue: £{daily_sales['revenue'].max():.2f}")
    
    # Sales by product
    product_sales = sales_df.groupby('productId').agg({
        'saleId': 'count',
        'unitPrice': ['sum', 'mean']
    })
    product_sales.columns = ['units_sold', 'total_revenue', 'avg_price']
    
    print(f"\nProduct Sales Summary:")
    print(f"  Most popular product: {product_sales['units_sold'].max():,} units")
    print(f"  Average units per product: {product_sales['units_sold'].mean():.1f}")
    
    # Monthly sales trend
    monthly_sales = sales_df.groupby(sales_df['saleDate'].dt.to_period('M')).agg({
        'saleId': 'count',
        'unitPrice': 'sum'
    }).rename(columns={'saleId': 'units_sold', 'unitPrice': 'revenue'})
    
    print(f"\nMonthly Sales Trend (first and last 3 months):")
    for month, row in list(monthly_sales.iterrows())[:3]:
        print(f"  {month}: {row['units_sold']:,} units, £{row['revenue']:,.2f}")
    print("  ...")
    for month, row in list(monthly_sales.iterrows())[-3:]:
        print(f"  {month}: {row['units_sold']:,} units, £{row['revenue']:,.2f}")
    
    return daily_sales, product_sales, monthly_sales

def create_visualizations(sales_df, daily_sales, monthly_sales):
    """Create sales visualizations"""
    print("\n=== Creating Visualizations ===")
    
    import matplotlib.pyplot as plt
    import seaborn as sns
    
    plt.style.use('seaborn-v0_8-darkgrid')
    fig, axes = plt.subplots(2, 2, figsize=(15, 10))
    
    # 1. Daily sales volume (last 90 days)
    ax1 = axes[0, 0]
    recent_daily = daily_sales.tail(90)
    recent_daily['units_sold'].plot(ax=ax1, alpha=0.7)
    ax1.set_xlabel('Date')
    ax1.set_ylabel('Units Sold')
    ax1.set_title('Daily Sales Volume (Last 90 Days)')
    ax1.tick_params(axis='x', rotation=45)
    
    # 2. Monthly sales trend
    ax2 = axes[0, 1]
    monthly_sales['units_sold'].plot(ax=ax2, marker='o')
    ax2.set_xlabel('Month')
    ax2.set_ylabel('Units Sold')
    ax2.set_title('Monthly Sales Volume')
    ax2.tick_params(axis='x', rotation=45)
    
    # Add trend line
    x_numeric = range(len(monthly_sales))
    z = np.polyfit(x_numeric, monthly_sales['units_sold'].values, 1)
    p = np.poly1d(z)
    ax2.plot(monthly_sales.index, p(x_numeric), "r--", alpha=0.8, 
             label=f'Trend: {z[0]:.0f} units/month')
    ax2.legend()
    
    # 3. Sales by day of week
    ax3 = axes[1, 0]
    sales_df['dow'] = sales_df['saleDate'].dt.day_name()
    dow_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    dow_sales = sales_df.groupby('dow').size().reindex(dow_order)
    dow_sales.plot(kind='bar', ax=ax3)
    ax3.set_xlabel('Day of Week')
    ax3.set_ylabel('Units Sold')
    ax3.set_title('Sales by Day of Week')
    ax3.tick_params(axis='x', rotation=45)
    
    # 4. Revenue distribution
    ax4 = axes[1, 1]
    monthly_sales['revenue'].plot(ax=ax4, marker='o', color='green')
    ax4.set_xlabel('Month')
    ax4.set_ylabel('Revenue (£)')
    ax4.set_title('Monthly Revenue')
    ax4.tick_params(axis='x', rotation=45)
    
    # Format y-axis for currency
    ax4.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'£{x:,.0f}'))
    
    plt.tight_layout()
    plt.savefig(DATA_DIR / 'sales_analysis.png', dpi=300, bbox_inches='tight')
    print("✓ Visualizations saved to sales_analysis.png")
    plt.close()

def save_results(sales_df, daily_sales, product_sales, monthly_sales):
    """Save sales data and analysis results"""
    print("\n=== Saving Results ===")
    
    # Save sales table
    sales_df.to_csv(DATA_DIR / 'sales.csv', index=False)
    print(f"✓ Saved {len(sales_df)} sales records to sales.csv")
    
    # Save aggregated data
    daily_sales.to_csv(DATA_DIR / 'daily_sales_summary.csv')
    print("✓ Saved daily sales summary to daily_sales_summary.csv")
    
    product_sales.to_csv(DATA_DIR / 'product_sales_summary.csv')
    print("✓ Saved product sales summary to product_sales_summary.csv")
    
    monthly_sales.to_csv(DATA_DIR / 'monthly_sales_summary.csv')
    print("✓ Saved monthly sales summary to monthly_sales_summary.csv")
    
    # Save summary statistics
    summary = {
        'generation_timestamp': datetime.now().isoformat(),
        'total_sales_records': len(sales_df),
        'unique_products': sales_df['productId'].nunique(),
        'unique_customers': sales_df['customerId'].nunique(),
        'date_range': {
            'start': str(sales_df['saleDate'].min().date()),
            'end': str(sales_df['saleDate'].max().date())
        },
        'total_revenue': float(sales_df['unitPrice'].sum()),
        'avg_daily_units': float(daily_sales['units_sold'].mean()),
        'avg_daily_revenue': float(daily_sales['revenue'].mean()),
        'sales_growth': {
            'first_month_units': int(monthly_sales.iloc[0]['units_sold']),
            'last_month_units': int(monthly_sales.iloc[-1]['units_sold']),
            'growth_pct': float((monthly_sales.iloc[-1]['units_sold'] - monthly_sales.iloc[0]['units_sold']) / monthly_sales.iloc[0]['units_sold'] * 100)
        }
    }
    
    with open(DATA_DIR / 'sales_generation_summary.json', 'w') as f:
        json.dump(summary, f, indent=2)
    print("✓ Saved summary to sales_generation_summary.json")

def main():
    """Main function"""
    print("=" * 60)
    print("Sales Table Generation")
    print("=" * 60)
    print(f"Execution time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Check prerequisites
    if not check_prerequisites():
        sys.exit(1)
    
    # Load data
    orders_df, order_items_df = load_data()
    
    # Generate sales table
    sales_df = generate_sales_table(orders_df, order_items_df)
    
    # Analyze sales
    daily_sales, product_sales, monthly_sales = analyze_sales(sales_df)
    
    # Create visualizations
    create_visualizations(sales_df, daily_sales, monthly_sales)
    
    # Save results
    save_results(sales_df, daily_sales, product_sales, monthly_sales)
    
    print("\n" + "=" * 60)
    print("✓ SALES GENERATION COMPLETE")
    print(f"\nGenerated {len(sales_df):,} sales records from delivered orders")
    print("Sales recorded on delivery dates as specified")
    print("\nNext step: Run script 07_final_validation.py")

if __name__ == "__main__":
    main()