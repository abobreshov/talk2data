#!/usr/bin/env python3
"""
Task 13: Simple Improved Product Mapping
This script creates an enhanced mapping to use more products from our catalog.
"""

import os
import sys
import pandas as pd
import numpy as np
import duckdb
from pathlib import Path
from datetime import datetime
import json
import pickle

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

# Load configuration
config_path = Path(__file__).parent / "config.json"
with open(config_path, 'r') as f:
    config = json.load(f)

DATA_DIR = Path(config['data_dir'])
PRODUCTS_DB = Path(config['products_db'])

def load_data():
    """Load products and M5 data"""
    print("=== Loading Data ===")
    
    # Load products
    conn = duckdb.connect(str(PRODUCTS_DB), read_only=True)
    our_products = conn.execute("""
        SELECT 
            sku as productId,
            name,
            brandName,
            sellingSize,
            price/100.0 as price_gbp,
            category,
            subcategory
        FROM products
        ORDER BY RANDOM()  -- Randomize for variety
    """).fetchdf()
    conn.close()
    
    print(f"✓ Loaded {len(our_products)} products")
    
    # Load M5 data
    m5_items = pd.read_csv(DATA_DIR / 'm5_unique_food_items.csv')
    m5_popularity = pd.read_csv(DATA_DIR / 'm5_item_popularity.csv')
    m5_prices = pd.read_csv(DATA_DIR / 'm5_price_stats_gbp.csv')
    
    m5_data = m5_items.merge(m5_popularity, on='item_id', how='left')
    m5_data = m5_data.merge(m5_prices, on='item_id', how='left')
    
    print(f"✓ Loaded {len(m5_data)} M5 items")
    
    return our_products, m5_data

def create_diverse_mapping(our_products, m5_data, target_coverage=0.8):
    """Create mapping aiming for better product coverage"""
    print("\n=== Creating Diverse Product Mapping ===")
    
    # Create price buckets for both datasets
    n_buckets = 20
    our_products['price_bucket'] = pd.qcut(
        our_products['price_gbp'], 
        q=n_buckets, 
        labels=False,
        duplicates='drop'
    )
    
    # For M5, use mean price or assign random bucket
    m5_data['price_bucket'] = pd.qcut(
        m5_data['mean'].fillna(our_products['price_gbp'].median()),
        q=n_buckets,
        labels=False,
        duplicates='drop'
    )
    
    # Sort M5 by popularity
    m5_data_sorted = m5_data.sort_values('total_sold', ascending=False).reset_index(drop=True)
    
    # Track product usage
    products_used = set()
    mapping_records = []
    
    # First pass: Map by price bucket with rotation
    bucket_indices = {i: 0 for i in range(n_buckets)}
    
    for idx, m5_item in m5_data_sorted.iterrows():
        bucket = m5_item['price_bucket']
        
        # Get products in this bucket
        bucket_products = our_products[our_products['price_bucket'] == bucket]
        
        if len(bucket_products) > 0:
            # Rotate through products in bucket
            product_idx = bucket_indices[bucket] % len(bucket_products)
            selected_product = bucket_products.iloc[product_idx]
            bucket_indices[bucket] += 1
        else:
            # Find nearest bucket with products
            for offset in range(1, n_buckets):
                for direction in [1, -1]:
                    alt_bucket = bucket + direction * offset
                    if 0 <= alt_bucket < n_buckets:
                        alt_products = our_products[our_products['price_bucket'] == alt_bucket]
                        if len(alt_products) > 0:
                            selected_product = alt_products.sample(1).iloc[0]
                            break
                if 'selected_product' in locals():
                    break
        
        if 'selected_product' in locals():
            products_used.add(selected_product['productId'])
            mapping_records.append({
                'm5_item_id': m5_item['item_id'],
                'm5_dept': m5_item.get('dept_id', ''),
                'm5_popularity_rank': idx + 1,
                'm5_total_sold': m5_item.get('total_sold', 0),
                'm5_median_price': m5_item.get('mean', None),
                'product_id': selected_product['productId'],
                'product_name': selected_product['name'],
                'brand': selected_product['brandName'],
                'category': selected_product['category'],
                'subcategory': selected_product['subcategory'],
                'price_gbp': selected_product['price_gbp']
            })
            del selected_product
    
    # Second pass: Ensure we use more products
    current_coverage = len(products_used) / len(our_products)
    print(f"First pass coverage: {current_coverage:.1%} ({len(products_used)} products)")
    
    if current_coverage < target_coverage and len(mapping_records) < len(m5_data):
        # Get unused products
        unused_products = our_products[~our_products['productId'].isin(products_used)]
        
        # Create additional mappings for least popular M5 items
        remaining_m5 = len(m5_data) - len(mapping_records)
        additional_mappings = min(len(unused_products), remaining_m5)
        
        if additional_mappings > 0:
            # Map remaining M5 items to unused products
            m5_remaining = m5_data_sorted.iloc[len(mapping_records):len(mapping_records)+additional_mappings]
            unused_sample = unused_products.sample(additional_mappings)
            
            for (idx, m5_item), (_, product) in zip(m5_remaining.iterrows(), unused_sample.iterrows()):
                products_used.add(product['productId'])
                mapping_records.append({
                    'm5_item_id': m5_item['item_id'],
                    'm5_dept': m5_item.get('dept_id', ''),
                    'm5_popularity_rank': len(mapping_records) + 1,
                    'm5_total_sold': m5_item.get('total_sold', 0),
                    'm5_median_price': m5_item.get('mean', None),
                    'product_id': product['productId'],
                    'product_name': product['name'],
                    'brand': product['brandName'],
                    'category': product['category'],
                    'subcategory': product['subcategory'],
                    'price_gbp': product['price_gbp']
                })
    
    mapping_df = pd.DataFrame(mapping_records)
    final_coverage = len(products_used) / len(our_products)
    
    print(f"\n✓ Created mapping for {len(mapping_df)} M5 items")
    print(f"✓ Final product coverage: {final_coverage:.1%} ({len(products_used)} products)")
    
    return mapping_df

def save_mapping(mapping_df):
    """Save the improved mapping"""
    print("\n=== Saving Mapping Files ===")
    
    # Backup original if exists
    original_path = DATA_DIR / 'm5_product_mapping.csv'
    if original_path.exists():
        backup_path = original_path.with_suffix('.csv.original')
        if not backup_path.exists():  # Only backup once
            os.rename(original_path, backup_path)
            print(f"✓ Backed up original to {backup_path.name}")
    
    # Save new mapping
    mapping_df.to_csv(original_path, index=False)
    print(f"✓ Saved mapping to {original_path.name}")
    
    # Create lookups
    m5_to_product = dict(zip(mapping_df['m5_item_id'], mapping_df['product_id']))
    product_to_m5 = {}
    for m5_id, prod_id in m5_to_product.items():
        if prod_id not in product_to_m5:
            product_to_m5[prod_id] = []
        product_to_m5[prod_id].append(m5_id)
    
    # Save pickle
    lookup_data = {
        'm5_to_product': m5_to_product,
        'product_to_m5': product_to_m5,
        'mapping_df': mapping_df,
        'mapping_timestamp': datetime.now().isoformat()
    }
    
    with open(DATA_DIR / 'm5_product_lookup.pkl', 'wb') as f:
        pickle.dump(lookup_data, f)
    
    # Save summary
    summary = {
        'total_mappings': len(mapping_df),
        'unique_products_used': mapping_df['product_id'].nunique(),
        'product_coverage_pct': float(mapping_df['product_id'].nunique() / 2501 * 100),
        'price_stats': {
            'mean': float(mapping_df['price_gbp'].mean()),
            'min': float(mapping_df['price_gbp'].min()),
            'max': float(mapping_df['price_gbp'].max()),
            'median': float(mapping_df['price_gbp'].median())
        }
    }
    
    with open(DATA_DIR / 'mapping_summary.json', 'w') as f:
        json.dump(summary, f, indent=2)
    
    print("✓ Saved lookup and summary files")

def main():
    """Main function"""
    print("=" * 60)
    print("Improved Product Mapping for Better Coverage")
    print("=" * 60)
    
    # Load data
    our_products, m5_data = load_data()
    
    # Create diverse mapping
    mapping_df = create_diverse_mapping(our_products, m5_data, target_coverage=0.8)
    
    # Validate
    print("\n=== Validation ===")
    print(f"Price correlation: {mapping_df[['m5_median_price', 'price_gbp']].corr().iloc[0,1]:.3f}")
    
    # Show category distribution
    cat_dist = mapping_df['category'].value_counts().head(10)
    print("\nTop categories in mapping:")
    for cat, count in cat_dist.items():
        print(f"  {cat}: {count}")
    
    # Save
    save_mapping(mapping_df)
    
    print("\n" + "=" * 60)
    print("✓ MAPPING COMPLETE")
    print(f"Previous coverage: 902 products (36.1%)")
    print(f"New coverage: {mapping_df['product_id'].nunique()} products ({mapping_df['product_id'].nunique()/2501*100:.1f}%)")

if __name__ == "__main__":
    main()