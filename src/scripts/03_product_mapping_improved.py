#!/usr/bin/env python3
"""
Task 13: Improved Product Mapping Strategy with Approximation
This script creates an enhanced mapping between M5 Walmart FOOD items and our products
using text similarity and category matching to maximize coverage.
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
import re
from difflib import SequenceMatcher
from collections import defaultdict

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

# Load configuration
config_path = Path(__file__).parent / "config.json"
with open(config_path, 'r') as f:
    config = json.load(f)

DATA_DIR = Path(config['data_dir'])
PRODUCTS_DB = Path(config['products_db'])

# Common food keywords for better matching
FOOD_KEYWORDS = {
    'dairy': ['milk', 'cheese', 'yogurt', 'butter', 'cream', 'dairy'],
    'meat': ['beef', 'pork', 'chicken', 'lamb', 'turkey', 'meat', 'bacon', 'sausage'],
    'seafood': ['fish', 'salmon', 'tuna', 'shrimp', 'seafood', 'cod', 'prawns'],
    'produce': ['apple', 'banana', 'orange', 'tomato', 'potato', 'carrot', 'lettuce', 'fruit', 'vegetable'],
    'bakery': ['bread', 'cake', 'cookie', 'pastry', 'muffin', 'donut', 'baked'],
    'frozen': ['frozen', 'ice cream', 'pizza'],
    'beverages': ['juice', 'soda', 'water', 'drink', 'coffee', 'tea'],
    'snacks': ['chips', 'crackers', 'popcorn', 'nuts', 'snack'],
    'grains': ['rice', 'pasta', 'cereal', 'oats', 'grain'],
    'condiments': ['sauce', 'ketchup', 'mayo', 'mustard', 'dressing', 'oil', 'vinegar']
}

def load_our_products():
    """Load products from our database with categories"""
    print("=== Loading Our Products Database ===")
    
    try:
        conn = duckdb.connect(str(PRODUCTS_DB), read_only=True)
        
        # Get all products with categories
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
            ORDER BY price_gbp
        """).fetchdf()
        
        # Clean up any empty strings
        our_products['category'] = our_products['category'].fillna('')
        our_products['subcategory'] = our_products['subcategory'].fillna('')
        
        print(f"✓ Loaded {len(our_products)} products from database")
        print(f"  - Price range: £{our_products['price_gbp'].min():.2f} - £{our_products['price_gbp'].max():.2f}")
        print(f"  - With category: {(our_products['category'] != '').sum()} products")
        print(f"  - With subcategory: {(our_products['subcategory'] != '').sum()} products")
        
        conn.close()
        return our_products
        
    except Exception as e:
        print(f"✗ Error loading products: {e}")
        sys.exit(1)

def normalize_text(text):
    """Normalize text for better matching"""
    if pd.isna(text):
        return ""
    # Convert to lowercase, remove extra spaces
    text = str(text).lower().strip()
    # Remove common words
    stop_words = {'the', 'a', 'an', 'and', 'or', 'with', 'in', 'on', 'for'}
    words = [w for w in text.split() if w not in stop_words]
    return ' '.join(words)

def extract_keywords(text):
    """Extract food-related keywords from text"""
    text = normalize_text(text)
    found_keywords = set()
    
    for category, keywords in FOOD_KEYWORDS.items():
        for keyword in keywords:
            if keyword in text:
                found_keywords.add(keyword)
                found_keywords.add(category)
    
    return found_keywords

def calculate_similarity(text1, text2):
    """Calculate similarity between two texts"""
    # Normalize texts
    text1 = normalize_text(text1)
    text2 = normalize_text(text2)
    
    # Basic sequence matching
    base_score = SequenceMatcher(None, text1, text2).ratio()
    
    # Keyword matching bonus
    keywords1 = extract_keywords(text1)
    keywords2 = extract_keywords(text2)
    
    if keywords1 and keywords2:
        keyword_overlap = len(keywords1.intersection(keywords2)) / max(len(keywords1), len(keywords2))
        base_score = base_score * 0.7 + keyword_overlap * 0.3
    
    return base_score

def create_price_tiers(products, n_tiers=10):
    """Create more granular price tiers for better matching"""
    products['price_tier_detailed'] = pd.qcut(
        products['price_gbp'], 
        q=n_tiers, 
        labels=[f'tier_{i}' for i in range(n_tiers)],
        duplicates='drop'
    )
    return products

def load_m5_data():
    """Load M5 analysis results with enhanced item information"""
    print("\n=== Loading M5 Analysis Data ===")
    
    # Load unique M5 items
    m5_items = pd.read_csv(DATA_DIR / 'm5_unique_food_items.csv')
    print(f"✓ Loaded {len(m5_items)} unique M5 FOOD items")
    
    # Load item popularity
    m5_popularity = pd.read_csv(DATA_DIR / 'm5_item_popularity.csv')
    
    # Load price statistics
    m5_prices = pd.read_csv(DATA_DIR / 'm5_price_stats_gbp.csv')
    
    # Merge all M5 data
    m5_data = m5_items.merge(m5_popularity, on='item_id', how='left')
    m5_data = m5_data.merge(m5_prices, on='item_id', how='left')
    
    # Extract item information from item_id
    # M5 item_id format: FOODS_X_XXX (e.g., FOODS_3_090)
    m5_data['m5_category'] = m5_data['item_id'].str.extract(r'FOODS_(\d+)_')[0]
    m5_data['m5_item_num'] = m5_data['item_id'].str.extract(r'FOODS_\d+_(\d+)')[0]
    
    # Normalize dept_id for matching
    if 'dept_id' in m5_data.columns:
        m5_data['dept_normalized'] = m5_data['dept_id'].str.replace('_', ' ').str.lower()
    
    return m5_data

def find_best_matches(m5_item, products, n_candidates=10):
    """Find best matching products for an M5 item using multiple criteria"""
    candidates = []
    
    # Get price range for filtering (±50% of median price)
    target_price = None
    if pd.notna(m5_item.get('median_price_gbp', None)):
        target_price = m5_item['median_price_gbp']
        price_min = target_price * 0.5
        price_max = target_price * 1.5
        price_filtered = products[
            (products['price_gbp'] >= price_min) & 
            (products['price_gbp'] <= price_max)
        ]
    else:
        price_filtered = products
        # Use average price as target if no M5 price available
        target_price = products['price_gbp'].median()
    
    # If we have department info, try to match with categories
    dept_score_boost = 0
    if 'dept_normalized' in m5_item and pd.notna(m5_item['dept_normalized']):
        dept_keywords = extract_keywords(m5_item['dept_normalized'])
        
        for idx, product in price_filtered.iterrows():
            score = 0
            
            # Category matching
            if product['category']:
                cat_keywords = extract_keywords(product['category'])
                if cat_keywords and dept_keywords:
                    score += len(cat_keywords.intersection(dept_keywords)) * 0.3
            
            # Subcategory matching
            if product['subcategory']:
                subcat_keywords = extract_keywords(product['subcategory'])
                if subcat_keywords and dept_keywords:
                    score += len(subcat_keywords.intersection(dept_keywords)) * 0.2
            
            # Name matching with keywords
            name_keywords = extract_keywords(product['name'])
            if name_keywords and dept_keywords:
                score += len(name_keywords.intersection(dept_keywords)) * 0.5
            
            if score > 0:
                candidates.append({
                    'product_idx': idx,
                    'score': score,
                    'price_diff': abs(product['price_gbp'] - target_price) if pd.notna(target_price) else 0
                })
    
    # If no good category matches, use price-based selection
    if len(candidates) < n_candidates:
        remaining = n_candidates - len(candidates)
        # Sample from appropriate price tier
        if len(price_filtered) > 0:
            sampled = price_filtered.sample(min(remaining, len(price_filtered)))
            for idx, product in sampled.iterrows():
                candidates.append({
                    'product_idx': idx,
                    'score': 0.1,  # Base score for price match
                    'price_diff': abs(product['price_gbp'] - target_price) if pd.notna(target_price) else 0
                })
    
    # Sort by score (desc) then by price difference (asc)
    candidates.sort(key=lambda x: (-x['score'], x['price_diff']))
    
    return candidates[:n_candidates]

def create_enhanced_mapping(m5_data, our_products):
    """Create mapping using approximation and similarity matching"""
    print("\n=== Creating Enhanced Product Mapping ===")
    
    # Add detailed price tiers
    our_products = create_price_tiers(our_products)
    
    # Track product usage to ensure diversity
    product_usage = defaultdict(int)
    max_usage_per_product = 3  # Limit how many times a product can be mapped
    
    mapping_records = []
    matched_by_method = defaultdict(int)
    
    # Sort M5 items by popularity
    m5_data_sorted = m5_data.sort_values('total_sold', ascending=False)
    
    for idx, m5_item in m5_data_sorted.iterrows():
        # Find candidate products
        candidates = find_best_matches(m5_item, our_products)
        
        # Select product considering usage limits
        selected_product = None
        method = 'none'
        
        for candidate in candidates:
            product_idx = candidate['product_idx']
            if product_usage[product_idx] < max_usage_per_product:
                selected_product = our_products.iloc[product_idx]
                product_usage[product_idx] += 1
                
                if candidate['score'] > 0.5:
                    method = 'category_match'
                elif candidate['score'] > 0:
                    method = 'keyword_match'
                else:
                    method = 'price_match'
                break
        
        # Fallback: random selection from unused products
        if selected_product is None:
            unused = our_products[
                our_products.index.isin([i for i, count in product_usage.items() if count == 0])
            ]
            if len(unused) > 0:
                selected_product = unused.sample(1).iloc[0]
                product_usage[selected_product.name] += 1
                method = 'random_unused'
        
        if selected_product is not None:
            mapping_records.append({
                'm5_item_id': m5_item['item_id'],
                'm5_dept': m5_item.get('dept_id', ''),
                'm5_category': m5_item.get('m5_category', ''),
                'm5_popularity_rank': idx + 1,
                'm5_total_sold': m5_item.get('total_sold', 0),
                'm5_median_price': m5_item.get('median_price_gbp', None),
                'product_id': selected_product['productId'],
                'product_name': selected_product['name'],
                'brand': selected_product['brandName'],
                'category': selected_product['category'],
                'subcategory': selected_product['subcategory'],
                'price_gbp': selected_product['price_gbp'],
                'price_tier': selected_product.get('price_tier_detailed', ''),
                'match_method': method
            })
            matched_by_method[method] += 1
    
    mapping_df = pd.DataFrame(mapping_records)
    
    print(f"\n✓ Created mapping for {len(mapping_df)} items")
    print(f"✓ Used {len(product_usage)} unique products ({len(product_usage)/len(our_products)*100:.1f}% of catalog)")
    
    print("\nMatching methods used:")
    for method, count in sorted(matched_by_method.items(), key=lambda x: -x[1]):
        print(f"  - {method}: {count} ({count/len(mapping_df)*100:.1f}%)")
    
    return mapping_df

def validate_enhanced_mapping(mapping_df, our_products):
    """Validate the enhanced mapping"""
    print("\n=== Validating Enhanced Mapping ===")
    
    # Product coverage
    unique_products_used = mapping_df['product_id'].nunique()
    coverage_pct = unique_products_used / len(our_products) * 100
    
    print(f"\nProduct Coverage:")
    print(f"  - Total products: {len(our_products)}")
    print(f"  - Products used: {unique_products_used}")
    print(f"  - Coverage: {coverage_pct:.1f}%")
    
    # Category distribution
    if 'category' in mapping_df.columns:
        cat_dist = mapping_df['category'].value_counts().head(10)
        print("\nTop 10 mapped categories:")
        for cat, count in cat_dist.items():
            if cat:  # Skip empty categories
                print(f"  - {cat}: {count} items")
    
    # Price distribution comparison
    print("\nPrice distribution:")
    print(f"  - M5 median prices: £{mapping_df['m5_median_price'].min():.2f} - £{mapping_df['m5_median_price'].max():.2f}")
    print(f"  - Mapped prices: £{mapping_df['price_gbp'].min():.2f} - £{mapping_df['price_gbp'].max():.2f}")
    print(f"  - Average price difference: £{abs(mapping_df['price_gbp'] - mapping_df['m5_median_price']).mean():.2f}")
    
    # Match quality
    match_quality = mapping_df['match_method'].value_counts()
    print("\nMatch quality distribution:")
    for method, count in match_quality.items():
        print(f"  - {method}: {count} ({count/len(mapping_df)*100:.1f}%)")
    
    return True

def save_enhanced_mapping(mapping_df, original_mapping_path):
    """Save enhanced mapping files"""
    print("\n=== Saving Enhanced Mapping Files ===")
    
    # Backup original mapping
    if original_mapping_path.exists():
        backup_path = original_mapping_path.with_suffix('.csv.backup')
        os.rename(original_mapping_path, backup_path)
        print(f"✓ Backed up original mapping to {backup_path.name}")
    
    # Save enhanced mapping
    mapping_df.to_csv(DATA_DIR / 'm5_product_mapping.csv', index=False)
    print(f"✓ Saved enhanced mapping to m5_product_mapping.csv ({len(mapping_df)} rows)")
    
    # Create lookup dictionaries
    m5_to_product = dict(zip(mapping_df['m5_item_id'], mapping_df['product_id']))
    product_to_m5 = defaultdict(list)
    for m5_id, prod_id in m5_to_product.items():
        product_to_m5[prod_id].append(m5_id)
    
    # Save lookup pickle
    lookup_data = {
        'm5_to_product': m5_to_product,
        'product_to_m5': dict(product_to_m5),
        'mapping_df': mapping_df,
        'mapping_timestamp': datetime.now().isoformat(),
        'mapping_version': 'enhanced_v2'
    }
    
    with open(DATA_DIR / 'm5_product_lookup.pkl', 'wb') as f:
        pickle.dump(lookup_data, f)
    print("✓ Saved enhanced lookup dictionaries")
    
    # Save summary statistics
    summary = {
        'total_mappings': len(mapping_df),
        'unique_products_used': mapping_df['product_id'].nunique(),
        'product_coverage_pct': float(mapping_df['product_id'].nunique() / len(mapping_df) * 100),
        'price_stats': {
            'mean': float(mapping_df['price_gbp'].mean()),
            'min': float(mapping_df['price_gbp'].min()),
            'max': float(mapping_df['price_gbp'].max()),
            'median': float(mapping_df['price_gbp'].median())
        },
        'match_methods': mapping_df['match_method'].value_counts().to_dict(),
        'categories_mapped': mapping_df[mapping_df['category'] != '']['category'].nunique()
    }
    
    with open(DATA_DIR / 'mapping_summary.json', 'w') as f:
        json.dump(summary, f, indent=2)
    print("✓ Saved enhanced mapping summary")

def main():
    """Main function"""
    print("=" * 60)
    print("Enhanced Product Mapping with Approximation")
    print("=" * 60)
    print(f"Execution time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Load our products with categories
    our_products = load_our_products()
    
    # Load M5 data
    m5_data = load_m5_data()
    
    # Create enhanced mapping
    mapping_df = create_enhanced_mapping(m5_data, our_products)
    
    # Validate mapping
    validate_enhanced_mapping(mapping_df, our_products)
    
    # Save mapping files
    original_mapping_path = DATA_DIR / 'm5_product_mapping.csv'
    save_enhanced_mapping(mapping_df, original_mapping_path)
    
    print("\n" + "=" * 60)
    print("✓ ENHANCED PRODUCT MAPPING COMPLETE")
    print("\nImprovement Summary:")
    print(f"  - Mapped {len(mapping_df)} M5 items")
    print(f"  - Used {mapping_df['product_id'].nunique()} unique products")
    print(f"  - Product catalog coverage: {mapping_df['product_id'].nunique()/len(our_products)*100:.1f}%")
    print(f"  - Category/keyword matches: {(mapping_df['match_method'].isin(['category_match', 'keyword_match'])).sum()}")
    print("\nNext step: Run script 04_customer_distribution.py")

if __name__ == "__main__":
    main()