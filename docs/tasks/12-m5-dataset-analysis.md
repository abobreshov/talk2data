# Task 12: M5 Dataset Loading and Analysis

## Objective
Load and analyze the M5 Walmart dataset using the datasetsforecast package to understand its structure and prepare for order synthesis.

## M5 Dataset Structure

### 1. Train DataFrame (walmart_orders_df)
- **Shape**: 47,649,940 rows
- **Columns**:
  - `unique_id`: Product-store combination (e.g., "FOODS_1_001_CA_1")
  - `ds`: Date (datetime64)
  - `y`: Sales quantity (float32)
- **Date Range**: 2011-01-29 to 2016-06-19
- **30,490 unique product-store combinations**

### 2. Exogenous DataFrame (X_df)
- **Columns**:
  - `unique_id`, `ds`: Same as train_df
  - `event_name_1/2`, `event_type_1/2`: Event information
  - `snap_CA`, `snap_TX`, `snap_WI`: SNAP days by state
  - `sell_price`: Product price in USD

### 3. Static DataFrame (S_df)
- **Mapping table** for product metadata:
  - `unique_id`: Full product-store ID
  - `item_id`: Product ID (e.g., "FOODS_1_001")
  - `dept_id`: Department (e.g., "FOODS_1")
  - `cat_id`: Category (FOODS, HOBBIES, HOUSEHOLD)
  - `store_id`: Store (CA_1, CA_2, etc.)
  - `state_id`: State (CA, TX, WI)

## Analysis Steps

### 1. Filter FOODS Category Data
```python
from datasetsforecast.m5 import M5
import pandas as pd
import numpy as np

# Load M5 dataset
train_df, X_df, S_df = M5.load(directory='data')

# Filter FOODS category
foods_ids = S_df[S_df['cat_id'] == 'FOODS']['unique_id'].unique()
foods_train = train_df[train_df['unique_id'].isin(foods_ids)]
foods_x = X_df[X_df['unique_id'].isin(foods_ids)]

print(f"Total FOODS records: {len(foods_train):,}")
print(f"Unique FOODS products: {foods_train['unique_id'].nunique()}")
print(f"Date range: {foods_train['ds'].min()} to {foods_train['ds'].max()}")
```

### 2. Extract Unique FOODS Items
```python
# Get unique FOOD items (without store suffix)
foods_items = S_df[S_df['cat_id'] == 'FOODS']['item_id'].unique()
print(f"Unique FOODS item types: {len(foods_items)}")

# Sample items
print("Sample FOODS items:")
print(foods_items[:10])
```

### 3. Analyze Sales Patterns
```python
# Daily sales aggregation
daily_sales = foods_train.groupby('ds').agg({
    'y': ['sum', 'mean', 'count']
}).reset_index()

# Items per transaction (approximate)
# Group by date and store to simulate "orders"
order_simulation = foods_train[foods_train['y'] > 0].groupby(['ds', 'unique_id']).agg({
    'y': 'sum'
}).reset_index()

# Extract store from unique_id
order_simulation['store'] = order_simulation['unique_id'].str.split('_').str[-2:].str.join('_')

# Estimate items per order
items_per_order = order_simulation.groupby(['ds', 'store']).size().describe()
print(f"\nEstimated items per order statistics:")
print(items_per_order)
```

### 4. Price Analysis
```python
# Get price information for FOODS
foods_prices = foods_x[['unique_id', 'sell_price']].drop_duplicates()

# Extract item_id for grouping
foods_prices['item_id'] = foods_prices['unique_id'].str.rsplit('_', n=2).str[0]

# Price statistics
price_stats = foods_prices.groupby('item_id')['sell_price'].agg(['min', 'max', 'mean'])
print(f"\nPrice statistics (USD):")
print(price_stats.describe())

# Convert to GBP (approximate 1 USD = 0.8 GBP)
price_stats_gbp = price_stats * 0.8
print(f"\nPrice statistics (GBP):")
print(price_stats_gbp.describe())
```

### 5. Temporal Patterns
```python
# Day of week patterns
foods_train['dow'] = pd.to_datetime(foods_train['ds']).dt.dayofweek
dow_patterns = foods_train.groupby('dow')['y'].agg(['sum', 'mean'])

# Monthly patterns
foods_train['month'] = pd.to_datetime(foods_train['ds']).dt.month
monthly_patterns = foods_train.groupby('month')['y'].agg(['sum', 'mean'])

print("\nDay of week patterns (0=Monday, 6=Sunday):")
print(dow_patterns)
```

## Key Findings Summary
- FOODS category has ~14,370 product-store combinations
- ~1,437 unique FOOD items across all stores
- Sales data spans 5.5 years (2011-2016)
- Will need to map these items to our FOOD products in products.duckdb
- Price range suitable for order generation (after USD->GBP conversion)