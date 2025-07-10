#!/usr/bin/env python3
"""
Task 14: Customer Distribution Algorithm
This script distributes orders across customers following the constraint of 
1-2 orders per week per customer over a 2-year period.
"""

import os
import sys
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
import json
import pickle
from tqdm import tqdm

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

# Load configuration
config_path = Path(__file__).parent / "config.json"
with open(config_path, 'r') as f:
    config = json.load(f)

DATA_DIR = Path(config['data_dir'])
CUSTOMERS_CSV = Path(config['customers_csv'])

def load_customers():
    """Load and prepare customer data"""
    print("=== Loading Customer Data ===")
    
    try:
        customers_df = pd.read_csv(CUSTOMERS_CSV)
        
        # Rename 'id' column to 'customerId' for consistency
        if 'id' in customers_df.columns and 'customerId' not in customers_df.columns:
            customers_df = customers_df.rename(columns={'id': 'customerId'})
            print("✓ Renamed 'id' column to 'customerId'")
        
        print(f"✓ Loaded {len(customers_df)} customers")
        print(f"  Columns: {', '.join(customers_df.columns)}")
        
        # Ensure customerId is string type
        customers_df['customerId'] = customers_df['customerId'].astype(str)
        
        return customers_df
        
    except Exception as e:
        print(f"✗ Error loading customers: {e}")
        sys.exit(1)

def define_date_range():
    """Define the 2-year date range for order generation"""
    print("\n=== Defining Date Range ===")
    
    # End date is today
    end_date = datetime.now().date()
    # Start date is 2 years ago
    start_date = end_date - timedelta(days=730)  # 2 years = 730 days
    
    print(f"Order date range: {start_date} to {end_date}")
    print(f"Total days: {(end_date - start_date).days}")
    print(f"Total weeks: {(end_date - start_date).days // 7}")
    
    # Create date range
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')
    
    return start_date, end_date, date_range

class CustomerBehavior:
    """Class to represent individual customer shopping behavior"""
    
    def __init__(self, customer_id, seed=None):
        self.customer_id = customer_id
        if seed:
            np.random.seed(seed)
        
        # Randomly assign shopping pattern (1 or 2 orders per week)
        # 60% shop once a week, 40% shop twice
        self.orders_per_week = np.random.choice([1, 2], p=[0.6, 0.4])
        
        # Preferred shopping days (0=Monday, 6=Sunday)
        if self.orders_per_week == 1:
            # Single weekly shop - typically weekend or end of week
            self.preferred_days = [np.random.choice([4, 5, 6], p=[0.3, 0.4, 0.3])]  # Fri, Sat, or Sun
        else:
            # Twice weekly - one weekend, one midweek
            weekend_day = np.random.choice([5, 6], p=[0.55, 0.45])  # Sat or Sun
            midweek_day = np.random.choice([1, 2, 3], p=[0.3, 0.4, 0.3])  # Tue, Wed, Thu
            self.preferred_days = [midweek_day, weekend_day]
        
        # Shopping consistency (how often they stick to their pattern)
        # Most customers are fairly consistent
        self.consistency = np.random.uniform(0.75, 0.95)
        
        # Vacation/break weeks (when they don't order)
        # 2-4 weeks per year
        vacation_weeks_per_year = np.random.randint(2, 5)
        self.vacation_weeks = np.random.choice(
            range(104),  # 104 weeks in 2 years
            size=vacation_weeks_per_year * 2,  # 2 years
            replace=False
        )
        
        # Occasional skip weeks (illness, travel, etc.)
        # 3-8 random weeks where they might skip
        self.skip_probability = np.random.uniform(0.02, 0.05)  # 2-5% chance to skip any week
        
    def __repr__(self):
        return f"Customer {self.customer_id}: {self.orders_per_week}/week, days {self.preferred_days}, consistency {self.consistency:.2f}"

def create_customer_behaviors(customers_df):
    """Create shopping behaviors for all customers"""
    print("\n=== Creating Customer Behaviors ===")
    
    customer_behaviors = {}
    
    # Use enumerate to create consistent random seeds
    for idx, customer_id in enumerate(customers_df['customerId']):
        # Use index as seed for reproducibility
        behavior = CustomerBehavior(customer_id, seed=idx)
        customer_behaviors[customer_id] = behavior
    
    # Analyze behavior distribution
    orders_per_week_dist = {}
    for behavior in customer_behaviors.values():
        opw = behavior.orders_per_week
        orders_per_week_dist[opw] = orders_per_week_dist.get(opw, 0) + 1
    
    print(f"✓ Created behaviors for {len(customer_behaviors)} customers")
    print("\nOrders per week distribution:")
    for opw, count in sorted(orders_per_week_dist.items()):
        pct = count / len(customer_behaviors) * 100
        print(f"  {opw} order(s)/week: {count} customers ({pct:.1f}%)")
    
    return customer_behaviors

def generate_customer_orders(customer_behavior, date_range):
    """Generate order dates for a single customer"""
    orders = []
    
    # Group dates by week
    dates_df = pd.DataFrame({'date': date_range})
    dates_df['week'] = dates_df['date'].dt.isocalendar().week
    dates_df['year'] = dates_df['date'].dt.year
    dates_df['week_year'] = dates_df['year'].astype(str) + '_' + dates_df['week'].astype(str).str.zfill(2)
    dates_df['dow'] = dates_df['date'].dt.dayofweek
    dates_df['week_number'] = (dates_df.index // 7)  # Sequential week number
    
    # Get unique weeks
    weeks = dates_df.groupby('week_number').first().reset_index()
    
    for week_idx, week_row in weeks.iterrows():
        # Skip vacation weeks
        if week_idx in customer_behavior.vacation_weeks:
            continue
        
        # Random skip (illness, travel, etc.)
        if np.random.random() < customer_behavior.skip_probability:
            continue
        
        # Get dates in this week
        week_dates = dates_df[dates_df['week_number'] == week_idx]
        
        # Determine if customer follows their pattern this week
        follow_pattern = np.random.random() < customer_behavior.consistency
        
        if follow_pattern:
            # Shop on preferred days
            for preferred_day in customer_behavior.preferred_days:
                matching_dates = week_dates[week_dates['dow'] == preferred_day]
                if len(matching_dates) > 0:
                    order_date = matching_dates.iloc[0]['date']
                    orders.append(order_date)
        else:
            # Random shopping pattern this week
            # Could be 0, 1, 2, or rarely 3 orders
            n_orders = np.random.choice([0, 1, 2, 3], p=[0.1, 0.5, 0.35, 0.05])
            
            if n_orders > 0 and len(week_dates) > 0:
                # Random days within the week
                selected_dates = week_dates.sample(min(n_orders, len(week_dates)))
                orders.extend(selected_dates['date'].tolist())
    
    return sorted(orders)

def generate_all_customer_orders(customer_behaviors, date_range):
    """Generate orders for all customers"""
    print("\n=== Generating Order Schedule ===")
    
    all_customer_orders = {}
    
    # Process each customer
    for customer_id, behavior in tqdm(customer_behaviors.items(), desc="Generating customer orders"):
        order_dates = generate_customer_orders(behavior, date_range)
        all_customer_orders[customer_id] = order_dates
    
    # Statistics
    order_counts = [len(orders) for orders in all_customer_orders.values()]
    
    print(f"\n✓ Generated orders for {len(all_customer_orders)} customers")
    print(f"Order count statistics:")
    print(f"  Total orders: {sum(order_counts):,}")
    print(f"  Average orders per customer: {np.mean(order_counts):.1f}")
    print(f"  Min orders: {min(order_counts)}")
    print(f"  Max orders: {max(order_counts)}")
    
    # Calculate orders per week
    total_weeks = len(date_range) / 7
    avg_orders_per_week = np.mean(order_counts) / total_weeks
    print(f"  Average orders per customer per week: {avg_orders_per_week:.2f}")
    
    return all_customer_orders

def create_order_schedule_dataframe(all_customer_orders, start_date, end_date):
    """Create DataFrame with order schedule"""
    print("\n=== Creating Order Schedule DataFrame ===")
    
    order_records = []
    order_id_counter = 1
    
    for customer_id, order_dates in all_customer_orders.items():
        for order_date in order_dates:
            # Generate delivery date (1-7 days after order)
            delivery_days = np.random.choice([1, 2, 3, 4, 5, 6, 7], 
                                           p=[0.15, 0.25, 0.25, 0.20, 0.10, 0.03, 0.02])
            # Convert numpy int64 to regular int
            delivery_days = int(delivery_days)
            delivery_date = order_date + timedelta(days=delivery_days)
            
            order_records.append({
                'orderId': f'ORD_{order_id_counter:08d}',
                'customerId': customer_id,
                'orderDate': order_date,
                'deliveryDate': delivery_date,
                'delivery_days': delivery_days
            })
            order_id_counter += 1
    
    # Create DataFrame
    orders_schedule_df = pd.DataFrame(order_records)
    
    # Add order status based on delivery date
    today = datetime.now().date()
    
    def determine_status(row):
        if row['deliveryDate'].date() > today:
            return 'FUTURE'
        elif row['deliveryDate'].date() == today:
            return 'PICKED'
        else:
            return 'DELIVERED'  # Will update 0.5% to CANCELLED later
    
    orders_schedule_df['orderStatus'] = orders_schedule_df.apply(determine_status, axis=1)
    
    print(f"✓ Created schedule with {len(orders_schedule_df):,} orders")
    
    return orders_schedule_df

def apply_cancellation_rate(orders_schedule_df):
    """Apply 0.5% cancellation rate to past orders"""
    print("\n=== Applying Cancellation Rate ===")
    
    today = datetime.now().date()
    
    # Get past orders (delivered status)
    past_orders = orders_schedule_df[
        orders_schedule_df['deliveryDate'].dt.date < today
    ]
    
    # Calculate number to cancel (0.5%)
    n_to_cancel = int(len(past_orders) * 0.005)
    
    print(f"Past orders: {len(past_orders):,}")
    print(f"Orders to cancel (0.5%): {n_to_cancel}")
    
    if n_to_cancel > 0:
        # Randomly select orders to cancel
        cancel_indices = past_orders.sample(n_to_cancel).index
        orders_schedule_df.loc[cancel_indices, 'orderStatus'] = 'CANCELLED'
    
    # Show final status distribution
    status_dist = orders_schedule_df['orderStatus'].value_counts()
    print("\nFinal order status distribution:")
    for status, count in status_dist.items():
        pct = count / len(orders_schedule_df) * 100
        print(f"  {status}: {count:,} ({pct:.1f}%)")
    
    return orders_schedule_df

def analyze_distribution(orders_schedule_df):
    """Analyze the order distribution"""
    print("\n=== Analyzing Order Distribution ===")
    
    # Customer statistics
    customer_stats = orders_schedule_df.groupby('customerId').agg({
        'orderId': 'count',
        'orderDate': ['min', 'max']
    }).round(2)
    
    customer_stats.columns = ['total_orders', 'first_order', 'last_order']
    customer_stats['days_active'] = (
        (customer_stats['last_order'] - customer_stats['first_order']).dt.days
    )
    customer_stats['weeks_active'] = customer_stats['days_active'] / 7
    customer_stats['orders_per_week'] = (
        customer_stats['total_orders'] / customer_stats['weeks_active']
    ).round(2)
    
    print("Customer order statistics:")
    print(customer_stats['orders_per_week'].describe())
    
    # Weekly order distribution
    orders_schedule_df['week'] = orders_schedule_df['orderDate'].dt.to_period('W')
    weekly_orders = orders_schedule_df.groupby('week').size()
    
    print(f"\nWeekly order statistics:")
    print(f"  Average orders per week: {weekly_orders.mean():.1f}")
    print(f"  Min orders per week: {weekly_orders.min()}")
    print(f"  Max orders per week: {weekly_orders.max()}")
    
    # Day of week distribution
    orders_schedule_df['dow'] = orders_schedule_df['orderDate'].dt.day_name()
    dow_dist = orders_schedule_df['dow'].value_counts()
    print("\nOrders by day of week:")
    for day in ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']:
        if day in dow_dist.index:
            count = dow_dist[day]
            pct = count / len(orders_schedule_df) * 100
            print(f"  {day}: {count:,} ({pct:.1f}%)")
    
    # Delivery time distribution
    delivery_dist = orders_schedule_df['delivery_days'].value_counts().sort_index()
    print("\nDelivery time distribution:")
    for days, count in delivery_dist.items():
        pct = count / len(orders_schedule_df) * 100
        print(f"  {days} day(s): {count:,} ({pct:.1f}%)")

def save_results(orders_schedule_df, customer_behaviors):
    """Save order schedule and customer behaviors"""
    print("\n=== Saving Results ===")
    
    # Save order schedule
    orders_schedule_df.to_csv(DATA_DIR / 'orders_schedule.csv', index=False)
    print(f"✓ Saved order schedule to orders_schedule.csv ({len(orders_schedule_df):,} orders)")
    
    # Save customer behaviors for reference
    with open(DATA_DIR / 'customer_behaviors.pkl', 'wb') as f:
        pickle.dump(customer_behaviors, f)
    print("✓ Saved customer behaviors to customer_behaviors.pkl")
    
    # Save summary statistics
    summary = {
        'generation_timestamp': datetime.now().isoformat(),
        'total_customers': len(customer_behaviors),
        'total_orders': len(orders_schedule_df),
        'date_range': {
            'start': str(orders_schedule_df['orderDate'].min().date()),
            'end': str(orders_schedule_df['orderDate'].max().date())
        },
        'avg_orders_per_customer': len(orders_schedule_df) / len(customer_behaviors),
        'status_distribution': orders_schedule_df['orderStatus'].value_counts().to_dict(),
        'delivery_days_distribution': orders_schedule_df['delivery_days'].value_counts().to_dict()
    }
    
    with open(DATA_DIR / 'customer_distribution_summary.json', 'w') as f:
        json.dump(summary, f, indent=2)
    print("✓ Saved summary to customer_distribution_summary.json")

def main():
    """Main function"""
    print("=" * 60)
    print("Customer Distribution Algorithm")
    print("=" * 60)
    print(f"Execution time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Load customers
    customers_df = load_customers()
    
    # Define date range
    start_date, end_date, date_range = define_date_range()
    
    # Create customer behaviors
    customer_behaviors = create_customer_behaviors(customers_df)
    
    # Generate orders for all customers
    all_customer_orders = generate_all_customer_orders(customer_behaviors, date_range)
    
    # Create order schedule DataFrame
    orders_schedule_df = create_order_schedule_dataframe(all_customer_orders, start_date, end_date)
    
    # Apply cancellation rate
    orders_schedule_df = apply_cancellation_rate(orders_schedule_df)
    
    # Analyze distribution
    analyze_distribution(orders_schedule_df)
    
    # Save results
    save_results(orders_schedule_df, customer_behaviors)
    
    print("\n" + "=" * 60)
    print("✓ CUSTOMER DISTRIBUTION COMPLETE")
    print(f"\nGenerated {len(orders_schedule_df):,} orders for {len(customers_df):,} customers")
    print("Next step: Run script 05_order_generation.py")

if __name__ == "__main__":
    main()