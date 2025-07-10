# Task 14: Customer Distribution Algorithm

## Objective
Design and implement an algorithm to distribute orders across customers following the constraint of 1-2 orders per week per customer over a 2-year period.

## Requirements Analysis
- **Time Period**: 2 years (104 weeks)
- **Orders per Customer**: 1-2 per week
- **Total Orders per Customer**: 104-208 over 2 years
- **Order Distribution**: Realistic patterns (weekly shopping habits)

## Implementation Steps

### 1. Load Customer Data
```python
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Load customers
customers_df = pd.read_csv('customers.csv')  # Adjust path as needed
print(f"Total customers: {len(customers_df)}")
print(f"Customer columns: {customers_df.columns.tolist()}")

# Extract customer IDs
customer_ids = customers_df['customerId'].unique()
n_customers = len(customer_ids)
```

### 2. Define Time Parameters
```python
# Set date range (2 years from today backwards)
end_date = datetime.now().date()
start_date = end_date - timedelta(days=730)  # 2 years

print(f"Order date range: {start_date} to {end_date}")
print(f"Total days: {(end_date - start_date).days}")
print(f"Total weeks: {(end_date - start_date).days // 7}")

# Create date range
date_range = pd.date_range(start=start_date, end=end_date, freq='D')
```

### 3. Create Customer Shopping Patterns
```python
# Define customer shopping behaviors
class CustomerBehavior:
    def __init__(self, customer_id):
        self.customer_id = customer_id
        
        # Randomly assign shopping pattern
        self.orders_per_week = np.random.choice([1, 2], p=[0.6, 0.4])  # 60% shop once, 40% shop twice
        
        # Preferred shopping days (0=Monday, 6=Sunday)
        if self.orders_per_week == 1:
            # Single weekly shop - typically weekend
            self.preferred_days = [np.random.choice([4, 5, 6])]  # Fri, Sat, or Sun
        else:
            # Twice weekly - one weekend, one midweek
            self.preferred_days = [
                np.random.choice([1, 2, 3]),  # Tue, Wed, Thu
                np.random.choice([5, 6])      # Sat, Sun
            ]
        
        # Shopping consistency (how often they stick to pattern)
        self.consistency = np.random.uniform(0.7, 0.95)  # 70-95% consistent
        
        # Vacation weeks (skip shopping)
        self.vacation_weeks = np.random.choice(range(104), size=np.random.randint(2, 5), replace=False)

# Create behavior for each customer
customer_behaviors = {
    cid: CustomerBehavior(cid) for cid in customer_ids
}
```

### 4. Generate Order Schedule
```python
def generate_customer_orders(customer_behavior, date_range):
    """Generate order dates for a single customer"""
    orders = []
    
    # Group dates by week
    dates_df = pd.DataFrame({'date': date_range})
    dates_df['week'] = dates_df['date'].dt.isocalendar().week
    dates_df['year'] = dates_df['date'].dt.year
    dates_df['week_year'] = dates_df['year'].astype(str) + '_' + dates_df['week'].astype(str).str.zfill(2)
    dates_df['dow'] = dates_df['date'].dt.dayofweek
    
    # Get unique weeks
    weeks = dates_df['week_year'].unique()
    
    for week_idx, week in enumerate(weeks):
        # Skip vacation weeks
        if week_idx in customer_behavior.vacation_weeks:
            continue
        
        # Get dates in this week
        week_dates = dates_df[dates_df['week_year'] == week]
        
        # Determine if customer shops this pattern or deviates
        follow_pattern = np.random.random() < customer_behavior.consistency
        
        if follow_pattern:
            # Shop on preferred days
            for preferred_day in customer_behavior.preferred_days:
                matching_dates = week_dates[week_dates['dow'] == preferred_day]
                if len(matching_dates) > 0:
                    orders.append(matching_dates.iloc[0]['date'])
        else:
            # Random shopping days this week
            n_orders = np.random.choice([0, 1, 2, 3], p=[0.1, 0.5, 0.35, 0.05])
            if n_orders > 0 and len(week_dates) > 0:
                selected_dates = week_dates.sample(min(n_orders, len(week_dates)))
                orders.extend(selected_dates['date'].tolist())
    
    return orders

# Generate orders for all customers
all_customer_orders = {}
for customer_id, behavior in customer_behaviors.items():
    order_dates = generate_customer_orders(behavior, date_range)
    all_customer_orders[customer_id] = order_dates
    
print(f"Generated orders for {len(all_customer_orders)} customers")
```

### 5. Create Order Distribution DataFrame
```python
# Flatten to create order records
order_records = []
order_id_counter = 1

for customer_id, order_dates in all_customer_orders.items():
    for order_date in order_dates:
        # Generate delivery date (1-7 days after order)
        delivery_days = np.random.randint(1, 8)
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
print(f"\nTotal orders generated: {len(orders_schedule_df)}")

# Add order status based on delivery date
today = datetime.now().date()
orders_schedule_df['orderStatus'] = orders_schedule_df.apply(
    lambda row: 'FUTURE' if row['deliveryDate'] > today else 
                'PICKED' if row['deliveryDate'] == today else 
                'DELIVERED',  # Will update 0.5% to CANCELLED later
    axis=1
)
```

### 6. Apply Cancellation Rate
```python
# Apply 0.5% cancellation rate to past orders
past_orders = orders_schedule_df[orders_schedule_df['deliveryDate'] < today]
n_to_cancel = int(len(past_orders) * 0.005)

# Randomly select orders to cancel
if n_to_cancel > 0:
    cancel_indices = past_orders.sample(n_to_cancel).index
    orders_schedule_df.loc[cancel_indices, 'orderStatus'] = 'CANCELLED'

print(f"\nOrder status distribution:")
print(orders_schedule_df['orderStatus'].value_counts())
```

### 7. Validate Distribution
```python
# Analyze order patterns
customer_stats = orders_schedule_df.groupby('customerId').agg({
    'orderId': 'count',
    'orderDate': ['min', 'max']
}).round(2)

customer_stats.columns = ['total_orders', 'first_order', 'last_order']
customer_stats['weeks_active'] = (
    (customer_stats['last_order'] - customer_stats['first_order']).dt.days / 7
).round(1)
customer_stats['orders_per_week'] = (
    customer_stats['total_orders'] / customer_stats['weeks_active']
).round(2)

print("\nCustomer order statistics:")
print(customer_stats['orders_per_week'].describe())

# Weekly order distribution
orders_schedule_df['week'] = pd.to_datetime(orders_schedule_df['orderDate']).dt.to_period('W')
weekly_orders = orders_schedule_df.groupby('week').size()

print(f"\nWeekly order statistics:")
print(f"Average orders per week: {weekly_orders.mean():.1f}")
print(f"Min orders per week: {weekly_orders.min()}")
print(f"Max orders per week: {weekly_orders.max()}")
```

### 8. Save Order Schedule
```python
# Save the order schedule (without items/prices yet)
orders_schedule_df.to_csv('src/data/orders_schedule.csv', index=False)
print(f"\nSaved order schedule to orders_schedule.csv")

# Save customer behaviors for reference
import pickle
with open('src/data/customer_behaviors.pkl', 'wb') as f:
    pickle.dump(customer_behaviors, f)
```

## Output Files
1. `orders_schedule.csv` - Order schedule with customer assignments
2. `customer_behaviors.pkl` - Customer shopping patterns

## Validation Metrics
- Average orders per customer per week: ~1.5
- Order distribution across week days
- Cancellation rate: 0.5%
- Status distribution matches business rules