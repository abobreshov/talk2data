# Task 13: Product Mapping Strategy

## Objective
Create a mapping between M5 Walmart FOOD items and our products database to enable realistic order synthesis.

## Implementation Steps

### 1. Load Our Products Database
```python
import duckdb
import pandas as pd
import numpy as np

# Connect to products database
conn = duckdb.connect('src/data/products.duckdb')

# Get FOOD products with prices in GBP
our_products = conn.execute("""
    SELECT 
        productId,
        sku,
        name,
        brandName,
        sellingSize,
        price/100.0 as price_gbp
    FROM products
    WHERE category = 'FOOD'
    ORDER BY price_gbp
""").fetchdf()

print(f"Total FOOD products in our database: {len(our_products)}")
print(f"Price range: £{our_products['price_gbp'].min():.2f} - £{our_products['price_gbp'].max():.2f}")
print(f"Average price: £{our_products['price_gbp'].mean():.2f}")
```

### 2. Analyze Product Price Distribution
```python
# Create price tiers for realistic shopping patterns
our_products['price_tier'] = pd.cut(
    our_products['price_gbp'],
    bins=[0, 2, 5, 10, 100],
    labels=['budget', 'standard', 'premium', 'luxury']
)

tier_distribution = our_products['price_tier'].value_counts()
print("\nOur products by price tier:")
print(tier_distribution)
```

### 3. Load M5 FOOD Items
```python
from datasetsforecast.m5 import M5

# Load M5 static data
_, _, S_df = M5.load(directory='data')

# Get unique FOOD items
foods_items = S_df[S_df['cat_id'] == 'FOODS']['item_id'].unique()
print(f"\nUnique M5 FOOD items to map: {len(foods_items)}")
```

### 4. Create Weighted Random Mapping
```python
# Shopping basket distribution (realistic patterns)
# Most purchases are budget/standard items
tier_weights = {
    'budget': 0.40,
    'standard': 0.35,
    'premium': 0.20,
    'luxury': 0.05
}

# Create mapping function
def create_product_mapping(m5_items, our_products_df, tier_weights):
    mapping = []
    
    for m5_item in m5_items:
        # Randomly select tier based on weights
        tier = np.random.choice(
            list(tier_weights.keys()),
            p=list(tier_weights.values())
        )
        
        # Get products from selected tier
        tier_products = our_products_df[our_products_df['price_tier'] == tier]
        
        if len(tier_products) > 0:
            # Randomly select a product from the tier
            selected_product = tier_products.sample(1).iloc[0]
            
            mapping.append({
                'm5_item_id': m5_item,
                'product_id': selected_product['productId'],
                'sku': selected_product['sku'],
                'product_name': selected_product['name'],
                'price_gbp': selected_product['price_gbp'],
                'price_tier': tier
            })
    
    return pd.DataFrame(mapping)

# Create the mapping
product_mapping = create_product_mapping(foods_items, our_products, tier_weights)

# Save mapping
product_mapping.to_csv('src/data/m5_product_mapping.csv', index=False)
print(f"\nMapping created for {len(product_mapping)} items")
```

### 5. Validate Mapping Distribution
```python
# Check price distribution of mapped products
mapping_stats = product_mapping['price_gbp'].describe()
print("\nMapped products price statistics (GBP):")
print(mapping_stats)

# Verify tier distribution matches target
actual_distribution = product_mapping['price_tier'].value_counts(normalize=True)
print("\nActual tier distribution in mapping:")
print(actual_distribution)

# Visualize price distribution
import matplotlib.pyplot as plt

plt.figure(figsize=(10, 6))
plt.subplot(1, 2, 1)
product_mapping['price_gbp'].hist(bins=50, edgecolor='black')
plt.xlabel('Price (GBP)')
plt.ylabel('Count')
plt.title('Price Distribution of Mapped Products')

plt.subplot(1, 2, 2)
product_mapping['price_tier'].value_counts().plot(kind='bar')
plt.xlabel('Price Tier')
plt.ylabel('Count')
plt.title('Product Distribution by Tier')
plt.xticks(rotation=45)

plt.tight_layout()
plt.savefig('src/data/product_mapping_distribution.png')
plt.close()
```

### 6. Create Lookup Functions
```python
# Create efficient lookup dictionary
m5_to_product = dict(zip(
    product_mapping['m5_item_id'],
    product_mapping['product_id']
))

# Create reverse lookup
product_to_m5 = dict(zip(
    product_mapping['product_id'],
    product_mapping['m5_item_id']
))

# Save as pickle for fast loading
import pickle

with open('src/data/m5_product_lookup.pkl', 'wb') as f:
    pickle.dump({
        'm5_to_product': m5_to_product,
        'product_to_m5': product_to_m5,
        'mapping_df': product_mapping
    }, f)

print("\nMapping files created:")
print("- m5_product_mapping.csv")
print("- m5_product_lookup.pkl")
print("- product_mapping_distribution.png")
```

## Output Files
1. `m5_product_mapping.csv` - Full mapping table
2. `m5_product_lookup.pkl` - Efficient lookup dictionaries
3. `product_mapping_distribution.png` - Visualization of mapping

## Validation Checklist
- [ ] All M5 FOOD items mapped to our products
- [ ] Price distribution follows realistic patterns
- [ ] Average mapped price suitable for £38 order target
- [ ] Tier distribution matches shopping patterns