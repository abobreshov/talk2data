# Task 15: Order Generation Logic

## Objective
Generate complete orders by combining M5 sales patterns with our product catalog, ensuring orders meet the £20-£100 range with growing average from £38 to £43 over 2 years.

## Implementation Steps

### 1. Load Required Data
```python
import pandas as pd
import numpy as np
import duckdb
import pickle
from datetime import datetime

# Load order schedule
orders_schedule = pd.read_csv('src/data/orders_schedule.csv')
orders_schedule['orderDate'] = pd.to_datetime(orders_schedule['orderDate'])
orders_schedule['deliveryDate'] = pd.to_datetime(orders_schedule['deliveryDate'])

# Load product mapping
with open('src/data/m5_product_lookup.pkl', 'rb') as f:
    lookup_data = pickle.load(f)
    m5_to_product = lookup_data['m5_to_product']
    mapping_df = lookup_data['mapping_df']

# Load M5 data
from datasetsforecast.m5 import M5
train_df, X_df, S_df = M5.load(directory='data')

# Filter FOODS only
foods_ids = S_df[S_df['cat_id'] == 'FOODS']['unique_id'].unique()
foods_train = train_df[train_df['unique_id'].isin(foods_ids)]

print(f"Orders to generate: {len(orders_schedule)}")
print(f"Product mappings available: {len(m5_to_product)}")
```

### 2. Analyze M5 Shopping Baskets
```python
# Create synthetic "baskets" from M5 data
# Group by date and store to simulate orders
def extract_m5_baskets(foods_train, min_items=5, max_items=50):
    """Extract shopping basket patterns from M5 data"""
    
    # Filter positive sales only
    positive_sales = foods_train[foods_train['y'] > 0].copy()
    
    # Extract store from unique_id
    positive_sales['store'] = positive_sales['unique_id'].str.split('_').str[-2:].str.join('_')
    positive_sales['item_id'] = positive_sales['unique_id'].str.rsplit('_', n=2).str[0]
    
    # Group by date and store to create baskets
    baskets = positive_sales.groupby(['ds', 'store']).agg({
        'item_id': list,
        'y': list
    }).reset_index()
    
    # Filter baskets by size
    baskets['n_items'] = baskets['item_id'].apply(len)
    baskets = baskets[
        (baskets['n_items'] >= min_items) & 
        (baskets['n_items'] <= max_items)
    ]
    
    return baskets

m5_baskets = extract_m5_baskets(foods_train)
print(f"M5 basket patterns extracted: {len(m5_baskets)}")
print(f"Items per basket: {m5_baskets['n_items'].describe()}")
```

### 3. Create Order Generation Function with Growing Basket Size
```python
def calculate_target_average(order_date, start_date, end_date):
    """Calculate target average order value based on date (£38 to £43 over 2 years)"""
    days_elapsed = (order_date - start_date).days
    total_days = (end_date - start_date).days
    progress = min(1.0, max(0.0, days_elapsed / total_days))
    
    # Linear growth from £38 to £43
    target_average = 38 + (5 * progress)
    
    # Add randomness: ±20% variation
    variation = np.random.uniform(0.8, 1.2)
    return target_average * variation

def generate_order_items(target_total, product_mapping_df, min_total=20, max_total=100):
    """Generate order items to meet target total"""
    
    items = []
    current_total = 0
    attempts = 0
    max_attempts = 100
    
    # Sort products by price for better selection
    available_products = product_mapping_df.sort_values('price_gbp').copy()
    
    while current_total < min_total and attempts < max_attempts:
        attempts += 1
        
        # Calculate remaining budget
        remaining = target_total - current_total
        
        if remaining < available_products['price_gbp'].min():
            # Can't add any more items
            break
        
        # Select products that fit in remaining budget
        suitable_products = available_products[
            available_products['price_gbp'] <= min(remaining, max_total - current_total)
        ]
        
        if len(suitable_products) == 0:
            break
        
        # Weight selection by price tier
        if remaining > 20:
            # More budget left, normal selection
            selected = suitable_products.sample(1).iloc[0]
        else:
            # Low budget, prefer cheaper items
            cheap_products = suitable_products[suitable_products['price_tier'] == 'budget']
            if len(cheap_products) > 0:
                selected = cheap_products.sample(1).iloc[0]
            else:
                selected = suitable_products.iloc[0]  # Cheapest available
        
        # Random quantity (most items quantity 1-3)
        quantity = np.random.choice([1, 2, 3], p=[0.7, 0.25, 0.05])
        
        # Check if adding this would exceed max
        item_total = selected['price_gbp'] * quantity
        if current_total + item_total > max_total:
            # Try quantity 1
            if current_total + selected['price_gbp'] <= max_total:
                quantity = 1
            else:
                continue
        
        items.append({
            'productId': selected['product_id'],
            'price': selected['price_gbp'],
            'quantity': quantity,
            'item_total': selected['price_gbp'] * quantity
        })
        
        current_total += selected['price_gbp'] * quantity
    
    return items, current_total

# Test the function
test_items, test_total = generate_order_items(38.0, mapping_df)
print(f"\nTest order: {len(test_items)} items, Total: £{test_total:.2f}")
```

### 4. Generate Orders with M5 Patterns and Growing Averages
```python
def generate_orders_from_m5_patterns(orders_schedule_df, m5_baskets, mapping_df, m5_to_product):
    """Generate order items using M5 basket patterns with growing basket sizes"""
    
    order_items_list = []
    orders_with_totals = []
    
    # Create a pool of basket patterns
    basket_pool = m5_baskets.sample(min(10000, len(m5_baskets)))
    
    # Get date range for progress calculation
    start_date = orders_schedule_df['orderDate'].min()
    end_date = orders_schedule_df['orderDate'].max()
    
    for idx, order in orders_schedule_df.iterrows():
        # Skip cancelled orders
        if order['orderStatus'] == 'CANCELLED':
            orders_with_totals.append({
                'orderId': order['orderId'],
                'customerId': order['customerId'],
                'orderStatus': order['orderStatus'],
                'orderDate': order['orderDate'],
                'deliveryDate': order['deliveryDate'],
                'totalPrice': 0.00
            })
            continue
        
        # Select a random M5 basket pattern
        basket = basket_pool.sample(1).iloc[0]
        m5_items = basket['item_id']
        quantities = basket['y']
        
        # Map M5 items to our products
        order_total = 0
        valid_items = []
        
        for m5_item, qty in zip(m5_items, quantities):
            if m5_item in m5_to_product:
                product_id = m5_to_product[m5_item]
                product_info = mapping_df[mapping_df['product_id'] == product_id].iloc[0]
                
                # Adjust quantity randomly (M5 quantities might be different)
                adjusted_qty = max(1, int(qty * np.random.uniform(0.5, 1.5)))
                
                item_total = product_info['price_gbp'] * adjusted_qty
                
                # Check if adding this keeps us in range
                if order_total + item_total <= 100:
                    valid_items.append({
                        'orderId': order['orderId'],
                        'productId': product_id,
                        'quantity': adjusted_qty,
                        'price': product_info['price_gbp']
                    })
                    order_total += item_total
        
        # Calculate target total for this order based on date
        target_order_value = calculate_target_average(
            order['orderDate'], 
            start_date, 
            end_date
        )
        
        # If order is too small, add more items
        if order_total < 20:
            additional_items, additional_total = generate_order_items(
                target_total=target_order_value - order_total,
                product_mapping_df=mapping_df,
                min_total=20 - order_total
            )
            
            for item in additional_items:
                valid_items.append({
                    'orderId': order['orderId'],
                    'productId': item['productId'],
                    'quantity': item['quantity'],
                    'price': item['price']
                })
            
            order_total += additional_total
        
        # Add to results
        order_items_list.extend(valid_items)
        orders_with_totals.append({
            'orderId': order['orderId'],
            'customerId': order['customerId'],
            'orderStatus': order['orderStatus'],
            'orderDate': order['orderDate'],
            'deliveryDate': order['deliveryDate'],
            'totalPrice': round(order_total, 2)
        })
        
        if idx % 1000 == 0:
            print(f"Processed {idx} orders...")
    
    return pd.DataFrame(orders_with_totals), pd.DataFrame(order_items_list)

# Generate all orders
print("Generating orders with items...")
orders_df, order_items_df = generate_orders_from_m5_patterns(
    orders_schedule, 
    m5_baskets, 
    mapping_df, 
    m5_to_product
)
```

### 5. Validate Order Totals
```python
# Check order value distribution
order_stats = orders_df[orders_df['orderStatus'] != 'CANCELLED']['totalPrice'].describe()
print("\nOrder value statistics (excluding cancelled):")
print(order_stats)

# Check if orders meet constraints
valid_orders = orders_df[
    (orders_df['orderStatus'] != 'CANCELLED') & 
    (orders_df['totalPrice'] >= 20) & 
    (orders_df['totalPrice'] <= 100)
]
print(f"\nOrders meeting £20-£100 constraint: {len(valid_orders)} / {len(orders_df[orders_df['orderStatus'] != 'CANCELLED'])}")
print(f"Percentage valid: {len(valid_orders) / len(orders_df[orders_df['orderStatus'] != 'CANCELLED']) * 100:.1f}%")

# Items per order
items_per_order = order_items_df.groupby('orderId').size()
print(f"\nItems per order statistics:")
print(items_per_order.describe())
```

### 6. Save to DuckDB
```python
# Connect to orders database
conn = duckdb.connect('src/data/orders.duckdb')

# Save orders table
conn.execute("DROP TABLE IF EXISTS orders")
conn.execute("CREATE TABLE orders AS SELECT * FROM orders_df")

# Save order_items table
conn.execute("DROP TABLE IF EXISTS order_items")
conn.execute("CREATE TABLE order_items AS SELECT * FROM order_items_df")

# Create indexes
conn.execute("CREATE INDEX idx_orders_customer ON orders(customerId)")
conn.execute("CREATE INDEX idx_orders_date ON orders(orderDate)")
conn.execute("CREATE INDEX idx_order_items_order ON order_items(orderId)")

# Verify data
print("\nDatabase summary:")
print(f"Orders: {conn.execute('SELECT COUNT(*) FROM orders').fetchone()[0]}")
print(f"Order items: {conn.execute('SELECT COUNT(*) FROM order_items').fetchone()[0]}")

conn.close()
```

## Output
- Orders table with complete order information
- Order_items table with product details
- All orders within £20-£100 range
- Average order value growing from £38 to £43 over 2 years
- Realistic item distributions based on M5 patterns