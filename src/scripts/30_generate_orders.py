#!/usr/bin/env python3
"""
Task 15: Order Generation Logic
This script generates order items for each order, creating realistic baskets
with growing average size from £38 to £43 over 2 years.
"""

import os
import sys
import pandas as pd
import numpy as np
import duckdb
from pathlib import Path
from datetime import datetime, timedelta
import json
import pickle
from tqdm import tqdm
import matplotlib.pyplot as plt

# Add scripts directory to path for imports
sys.path.append(str(Path(__file__).parent))

# Import configuration
from utilities.config import DATA_DIR, PRODUCTS_DB, ORDERS_DB, CUSTOMERS_CSV, get_config

# Get config for compatibility
config = get_config()

class BasketGenerator:
    """Generate realistic shopping baskets"""
    
    def __init__(self, products_df, mapping_df):
        self.products_df = products_df
        self.mapping_df = mapping_df
        
        # Create product lookup by tier
        self.products_by_tier = {
            tier: products_df[products_df['price_tier'] == tier].copy()
            for tier in ['budget', 'standard', 'premium', 'luxury']
        }
        
        # Shopping patterns - what types of items go together
        self.basket_patterns = {
            'essentials': {
                'tiers': {'budget': 0.5, 'standard': 0.4, 'premium': 0.1},
                'min_items': 8,
                'max_items': 15
            },
            'weekly_shop': {
                'tiers': {'budget': 0.35, 'standard': 0.4, 'premium': 0.2, 'luxury': 0.05},
                'min_items': 15,
                'max_items': 30
            },
            'top_up': {
                'tiers': {'budget': 0.3, 'standard': 0.5, 'premium': 0.2},
                'min_items': 3,
                'max_items': 8
            },
            'special_occasion': {
                'tiers': {'standard': 0.3, 'premium': 0.5, 'luxury': 0.2},
                'min_items': 10,
                'max_items': 20
            }
        }
    
    def get_target_basket_size(self, order_date, start_date, end_date):
        """Calculate target basket size based on date (growing from £38 to £43)"""
        # Linear growth over 2 years
        days_total = (end_date - start_date).days
        days_elapsed = (order_date - start_date).days
        progress = days_elapsed / days_total
        
        # Target grows from £38 to £43
        min_basket = 38.0
        max_basket = 43.0
        target = min_basket + (max_basket - min_basket) * progress
        
        # Add some randomness (±15%)
        variation = np.random.uniform(0.85, 1.15)
        target = target * variation
        
        # Ensure within £20-100 range
        return max(20.0, min(100.0, target))
    
    def select_basket_pattern(self, target_value):
        """Select appropriate basket pattern based on target value"""
        if target_value < 30:
            return 'top_up'
        elif target_value < 50:
            return 'essentials'
        elif target_value < 70:
            return 'weekly_shop'
        else:
            # Mix of weekly shop and special occasion for higher values
            return np.random.choice(['weekly_shop', 'special_occasion'], p=[0.7, 0.3])
    
    def generate_basket(self, order_date, start_date, end_date, customer_behavior=None):
        """Generate a single basket of items"""
        target_value = self.get_target_basket_size(order_date, start_date, end_date)
        pattern_name = self.select_basket_pattern(target_value)
        pattern = self.basket_patterns[pattern_name]
        
        basket_items = []
        current_total = 0.0
        attempts = 0
        max_attempts = 100
        
        # Determine number of items
        n_items = np.random.randint(pattern['min_items'], pattern['max_items'] + 1)
        
        while current_total < target_value * 0.9 and attempts < max_attempts:
            attempts += 1
            
            # Select tier based on pattern
            tier = np.random.choice(
                list(pattern['tiers'].keys()),
                p=list(pattern['tiers'].values())
            )
            
            tier_products = self.products_by_tier.get(tier)
            if tier_products is None or len(tier_products) == 0:
                continue
            
            # Select product using weighted sampling
            # Create weights that favor products at the beginning (cheaper items)
            # Use inverse rank weighting: weight = 1 / (rank + 1)
            weights = 1.0 / (np.arange(len(tier_products)) + 1)
            weights = weights / weights.sum()  # Normalize to probabilities
            
            idx = np.random.choice(len(tier_products), p=weights)
            product = tier_products.iloc[idx]
            
            # Determine quantity
            if product['price_gbp'] < 2:  # Cheap items - can buy multiple
                quantity = np.random.choice([1, 2, 3, 4], p=[0.4, 0.3, 0.2, 0.1])
            elif product['price_gbp'] < 5:  # Standard items
                quantity = np.random.choice([1, 2, 3], p=[0.7, 0.25, 0.05])
            else:  # Expensive items - usually just one
                quantity = 1
            
            # Check if adding this would exceed target too much
            item_total = product['price_gbp'] * quantity
            if current_total + item_total > target_value * 1.2:  # Allow 20% over
                # Try with quantity 1
                if current_total + product['price_gbp'] <= target_value * 1.2:
                    quantity = 1
                    item_total = product['price_gbp']
                else:
                    continue
            
            # Add to basket
            basket_items.append({
                'productId': product['productId'],
                'sku': product['sku'],
                'quantity': quantity,
                'unitPrice': product['price_gbp'],
                'totalPrice': item_total,
                'name': product['name'],
                'price_tier': tier
            })
            
            current_total += item_total
            
            # If we have enough items and are close to target, stop
            if len(basket_items) >= n_items and current_total >= target_value * 0.9:
                break
        
        # If basket is too small, add a few more items
        while current_total < target_value * 0.8 and len(basket_items) < 50:
            # Add budget/standard items to reach target
            tier = np.random.choice(['budget', 'standard'], p=[0.6, 0.4])
            tier_products = self.products_by_tier.get(tier)
            
            if tier_products is not None and len(tier_products) > 0:
                product = tier_products.sample(1).iloc[0]
                quantity = 1
                
                basket_items.append({
                    'productId': product['productId'],
                    'sku': product['sku'],
                    'quantity': quantity,
                    'unitPrice': product['price_gbp'],
                    'totalPrice': product['price_gbp'],
                    'name': product['name'],
                    'price_tier': tier
                })
                
                current_total += product['price_gbp']
        
        return basket_items, current_total

def load_data():
    """Load all necessary data"""
    print("=== Loading Data ===")
    
    # Load order schedule
    orders_schedule = pd.read_csv(DATA_DIR / 'orders_schedule.csv')
    orders_schedule['orderDate'] = pd.to_datetime(orders_schedule['orderDate'])
    orders_schedule['deliveryDate'] = pd.to_datetime(orders_schedule['deliveryDate'])
    print(f"✓ Loaded {len(orders_schedule)} orders from schedule")
    
    # Load products
    conn = duckdb.connect(str(PRODUCTS_DB), read_only=True)
    products_df = conn.execute("""
        SELECT 
            sku as productId,
            sku,
            name,
            brandName,
            sellingSize,
            price/100.0 as price_gbp
        FROM products
        ORDER BY price_gbp
    """).fetchdf()
    conn.close()
    
    # Add price tiers
    products_df['price_tier'] = pd.cut(
        products_df['price_gbp'],
        bins=[0, 2, 5, 10, 100],
        labels=['budget', 'standard', 'premium', 'luxury']
    )
    print(f"✓ Loaded {len(products_df)} products")
    
    # Load product mapping
    mapping_df = pd.read_csv(DATA_DIR / 'm5_product_mapping.csv')
    print(f"✓ Loaded product mapping ({len(mapping_df)} mappings)")
    
    # Try to load customer behaviors, but handle if not available
    customer_behaviors = {}
    try:
        # Instead of pickle, we can reconstruct behaviors from the orders schedule
        # Group by customer to get their order patterns
        customer_order_counts = orders_schedule.groupby('customerId').size()
        for customer_id in customer_order_counts.index:
            customer_behaviors[customer_id] = {
                'customer_id': customer_id,
                'order_count': customer_order_counts[customer_id]
            }
        print(f"✓ Reconstructed behaviors for {len(customer_behaviors)} customers")
    except Exception as e:
        print(f"⚠ Warning: Could not load customer behaviors: {e}")
        print("  Using empty customer behaviors - basket generation will proceed with defaults")
    
    return orders_schedule, products_df, mapping_df, customer_behaviors

def generate_all_orders(orders_schedule, products_df, mapping_df, customer_behaviors):
    """Generate order items for all orders"""
    print("\n=== Generating Order Items ===")
    
    # Initialize basket generator
    basket_gen = BasketGenerator(products_df, mapping_df)
    
    # Get date range
    start_date = orders_schedule['orderDate'].min()
    end_date = orders_schedule['orderDate'].max()
    
    # Storage for results
    all_orders = []
    all_order_items = []
    order_values = []
    
    # Group by customer for better progress tracking
    customer_groups = orders_schedule.groupby('customerId')
    
    for customer_id, customer_orders in tqdm(customer_groups, desc="Processing customers"):
        customer_behavior = customer_behaviors.get(customer_id)
        
        for _, order in customer_orders.iterrows():
            # Skip cancelled orders - they won't have items
            if order['orderStatus'] == 'CANCELLED':
                # Add order with 0 value
                all_orders.append({
                    'orderId': order['orderId'],
                    'customerId': order['customerId'],
                    'orderDate': order['orderDate'],
                    'deliveryDate': order['deliveryDate'],
                    'orderStatus': order['orderStatus'],
                    'totalAmount': 0.0,
                    'itemCount': 0
                })
                continue
            
            # Generate basket
            basket_items, total_value = basket_gen.generate_basket(
                order['orderDate'].to_pydatetime().date(),
                start_date.date(),
                end_date.date(),
                customer_behavior
            )
            
            # Create order record
            order_record = {
                'orderId': order['orderId'],
                'customerId': order['customerId'],
                'orderDate': order['orderDate'],
                'deliveryDate': order['deliveryDate'],
                'orderStatus': order['orderStatus'],
                'totalAmount': round(total_value, 2),
                'itemCount': len(basket_items)
            }
            all_orders.append(order_record)
            order_values.append(total_value)
            
            # Create order items
            for item_idx, item in enumerate(basket_items):
                order_item = {
                    'orderItemId': f"{order['orderId']}_ITEM_{item_idx+1:03d}",
                    'orderId': order['orderId'],
                    'productId': item['productId'],
                    'quantity': item['quantity'],
                    'unitPrice': item['unitPrice'],
                    'totalPrice': item['totalPrice']
                }
                all_order_items.append(order_item)
    
    # Convert to DataFrames
    orders_df = pd.DataFrame(all_orders)
    order_items_df = pd.DataFrame(all_order_items)
    
    print(f"\n✓ Generated {len(orders_df)} orders with {len(order_items_df)} items")
    print(f"  Average basket value: £{np.mean(order_values):.2f}")
    print(f"  Average items per order: {order_items_df.groupby('orderId').size().mean():.1f}")
    
    return orders_df, order_items_df, order_values

def analyze_results(orders_df, order_items_df, order_values):
    """Analyze the generated orders"""
    print("\n=== Analyzing Generated Orders ===")
    
    # Filter non-cancelled orders for value analysis
    active_orders = orders_df[orders_df['orderStatus'] != 'CANCELLED']
    active_values = [v for i, v in enumerate(order_values) 
                     if i < len(orders_df) and orders_df.iloc[i]['orderStatus'] != 'CANCELLED']
    
    # Basic statistics
    print("\nOrder value statistics (excluding cancelled):")
    print(f"  Count: {len(active_values)}")
    print(f"  Mean: £{np.mean(active_values):.2f}")
    print(f"  Std: £{np.std(active_values):.2f}")
    print(f"  Min: £{np.min(active_values):.2f}")
    print(f"  25%: £{np.percentile(active_values, 25):.2f}")
    print(f"  50%: £{np.percentile(active_values, 50):.2f}")
    print(f"  75%: £{np.percentile(active_values, 75):.2f}")
    print(f"  Max: £{np.max(active_values):.2f}")
    
    # Check growth over time
    active_orders['orderMonth'] = active_orders['orderDate'].dt.to_period('M')
    monthly_avg = active_orders.groupby('orderMonth')['totalAmount'].mean()
    
    print("\nMonthly average basket value (first and last 3 months):")
    for month, avg in list(monthly_avg.items())[:3]:
        print(f"  {month}: £{avg:.2f}")
    print("  ...")
    for month, avg in list(monthly_avg.items())[-3:]:
        print(f"  {month}: £{avg:.2f}")
    
    # Items per order
    items_per_order = order_items_df.groupby('orderId').size()
    print(f"\nItems per order:")
    print(f"  Mean: {items_per_order.mean():.1f}")
    print(f"  Min: {items_per_order.min()}")
    print(f"  Max: {items_per_order.max()}")
    
    # Product popularity
    product_orders = order_items_df.groupby('productId').agg({
        'quantity': 'sum',
        'orderId': 'nunique'
    }).rename(columns={'orderId': 'order_count'})
    
    print(f"\nProduct statistics:")
    print(f"  Unique products ordered: {len(product_orders)}")
    print(f"  Most popular product: {product_orders['quantity'].max()} units")
    
    return monthly_avg

def create_visualizations(orders_df, order_items_df, monthly_avg):
    """Create visualizations of order generation"""
    print("\n=== Creating Visualizations ===")
    
    plt.style.use('seaborn-v0_8-darkgrid')
    fig, axes = plt.subplots(2, 2, figsize=(15, 10))
    
    # Filter active orders
    active_orders = orders_df[orders_df['orderStatus'] != 'CANCELLED']
    
    # 1. Order value distribution
    ax1 = axes[0, 0]
    ax1.hist(active_orders['totalAmount'], bins=50, edgecolor='black', alpha=0.7)
    ax1.axvline(active_orders['totalAmount'].mean(), color='red', linestyle='--', 
                label=f'Mean: £{active_orders["totalAmount"].mean():.2f}')
    ax1.set_xlabel('Order Value (£)')
    ax1.set_ylabel('Number of Orders')
    ax1.set_title('Order Value Distribution')
    ax1.legend()
    
    # 2. Monthly average trend
    ax2 = axes[0, 1]
    monthly_avg.plot(ax=ax2, marker='o')
    ax2.set_xlabel('Month')
    ax2.set_ylabel('Average Order Value (£)')
    ax2.set_title('Average Basket Value Over Time')
    ax2.tick_params(axis='x', rotation=45)
    
    # Add trend line
    x_numeric = range(len(monthly_avg))
    z = np.polyfit(x_numeric, monthly_avg.values, 1)
    p = np.poly1d(z)
    ax2.plot(monthly_avg.index, p(x_numeric), "r--", alpha=0.8, 
             label=f'Trend: £{z[0]:.2f}/month')
    ax2.legend()
    
    # 3. Items per order distribution
    ax3 = axes[1, 0]
    items_per_order = active_orders['itemCount']
    ax3.hist(items_per_order, bins=range(0, items_per_order.max() + 2), 
             edgecolor='black', alpha=0.7)
    ax3.axvline(items_per_order.mean(), color='red', linestyle='--',
                label=f'Mean: {items_per_order.mean():.1f} items')
    ax3.set_xlabel('Number of Items')
    ax3.set_ylabel('Number of Orders')
    ax3.set_title('Items per Order Distribution')
    ax3.legend()
    
    # 4. Daily order volume
    ax4 = axes[1, 1]
    daily_orders = active_orders.groupby(active_orders['orderDate'].dt.date).size()
    # Show last 90 days
    daily_orders.tail(90).plot(ax=ax4, alpha=0.7)
    ax4.set_xlabel('Date')
    ax4.set_ylabel('Number of Orders')
    ax4.set_title('Daily Order Volume (Last 90 Days)')
    ax4.tick_params(axis='x', rotation=45)
    
    plt.tight_layout()
    plt.savefig(DATA_DIR / 'order_generation_analysis.png', dpi=300, bbox_inches='tight')
    print("✓ Saved visualization to order_generation_analysis.png")
    plt.close()

def save_results(orders_df, order_items_df):
    """Save the generated orders and order items"""
    print("\n=== Saving Results ===")
    
    # Save orders
    orders_df.to_csv(DATA_DIR / 'orders.csv', index=False)
    print(f"✓ Saved {len(orders_df)} orders to orders.csv")
    
    # Save order items
    order_items_df.to_csv(DATA_DIR / 'order_items.csv', index=False)
    print(f"✓ Saved {len(order_items_df)} order items to order_items.csv")
    
    # Save summary
    summary = {
        'generation_timestamp': datetime.now().isoformat(),
        'total_orders': len(orders_df),
        'total_order_items': len(order_items_df),
        'active_orders': len(orders_df[orders_df['orderStatus'] != 'CANCELLED']),
        'cancelled_orders': len(orders_df[orders_df['orderStatus'] == 'CANCELLED']),
        'avg_basket_value': float(orders_df[orders_df['orderStatus'] != 'CANCELLED']['totalAmount'].mean()),
        'avg_items_per_order': float(order_items_df.groupby('orderId').size().mean()),
        'unique_products_ordered': order_items_df['productId'].nunique(),
        'date_range': {
            'start': str(orders_df['orderDate'].min().date()),
            'end': str(orders_df['orderDate'].max().date())
        }
    }
    
    with open(DATA_DIR / 'order_generation_summary.json', 'w') as f:
        json.dump(summary, f, indent=2)
    print("✓ Saved summary to order_generation_summary.json")

def check_prerequisites():
    """Check if required files from previous steps exist"""
    required_files = [
        (DATA_DIR / 'orders_schedule.csv', 'Customer distribution (Task 14)'),
        (DATA_DIR / 'm5_product_mapping.csv', 'Product mapping (Task 13)'),
        (PRODUCTS_DB, 'Products database')
    ]
    
    missing = []
    for file_path, description in required_files:
        if not file_path.exists():
            missing.append(f"  - {file_path.name} ({description})")
    
    if missing:
        print("✗ Missing required files from previous steps:")
        for item in missing:
            print(item)
        print("\nPlease run the previous tasks in order:")
        print("  1. Task 11: Environment setup (01_environment_setup.py)")
        print("  2. Task 12: M5 dataset analysis (02_m5_dataset_analysis.py)")
        print("  3. Task 13: Product mapping (03_product_mapping.py)")
        print("  4. Task 14: Customer distribution (04_customer_distribution.py)")
        return False
    
    return True

def main():
    """Main function"""
    print("=" * 60)
    print("Order Generation Logic")
    print("=" * 60)
    print(f"Execution time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Check prerequisites
    if not check_prerequisites():
        sys.exit(1)
    
    # Load all data
    orders_schedule, products_df, mapping_df, customer_behaviors = load_data()
    
    # Generate orders
    orders_df, order_items_df, order_values = generate_all_orders(
        orders_schedule, products_df, mapping_df, customer_behaviors
    )
    
    # Analyze results
    monthly_avg = analyze_results(orders_df, order_items_df, order_values)
    
    # Create visualizations
    create_visualizations(orders_df, order_items_df, monthly_avg)
    
    # Save results
    save_results(orders_df, order_items_df)
    
    print("\n" + "=" * 60)
    print("✓ ORDER GENERATION COMPLETE")
    print(f"\nGenerated {len(orders_df)} orders with {len(order_items_df)} items")
    print("Next step: Run script 06_sales_generation.py")

if __name__ == "__main__":
    main()