#!/usr/bin/env python3
"""
Task 13: Product Mapping Strategy
This script creates a mapping between M5 Walmart FOOD items and our products database
to enable realistic order synthesis.
"""

import os
import sys
import pandas as pd
import numpy as np
import duckdb
from pathlib import Path
from datetime import datetime
import matplotlib.pyplot as plt
import pickle

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))
sys.path.append(str(Path(__file__).parent))

# Load configuration from environment
from utilities.config import DATA_DIR, PRODUCTS_DB, ORDERS_DB, CUSTOMERS_CSV, get_config
config = get_config()  # For compatibility

def load_our_products():
    """Load products from our database"""
    print("=== Loading Our Products Database ===")
    
    try:
        conn = duckdb.connect(str(PRODUCTS_DB), read_only=True)
        
        # Get all products (they're all FOOD category)
        our_products = conn.execute("""
            SELECT 
                sku as productId,  -- Using SKU as productId
                sku,
                name,
                brandName,
                sellingSize,
                price/100.0 as price_gbp  -- Convert pennies to GBP
            FROM products
            ORDER BY price_gbp
        """).fetchdf()
        
        print(f"✓ Loaded {len(our_products)} products from database")
        print(f"  - Price range: £{our_products['price_gbp'].min():.2f} - £{our_products['price_gbp'].max():.2f}")
        print(f"  - Average price: £{our_products['price_gbp'].mean():.2f}")
        print(f"  - Unique brands: {our_products['brandName'].nunique()}")
        
        conn.close()
        return our_products
        
    except Exception as e:
        print(f"✗ Error loading products: {e}")
        sys.exit(1)

def analyze_product_distribution(our_products):
    """Analyze our product price distribution"""
    print("\n=== Analyzing Product Price Distribution ===")
    
    # Create price tiers for realistic shopping patterns
    our_products['price_tier'] = pd.cut(
        our_products['price_gbp'],
        bins=[0, 2, 5, 10, 100],
        labels=['budget', 'standard', 'premium', 'luxury']
    )
    
    tier_distribution = our_products['price_tier'].value_counts()
    tier_percentages = our_products['price_tier'].value_counts(normalize=True) * 100
    
    print("\nOur products by price tier:")
    for tier in ['budget', 'standard', 'premium', 'luxury']:
        count = tier_distribution.get(tier, 0)
        pct = tier_percentages.get(tier, 0)
        print(f"  - {tier}: {count} products ({pct:.1f}%)")
    
    # Analyze brands by tier
    print("\nTop brands by tier:")
    for tier in ['budget', 'standard', 'premium', 'luxury']:
        tier_products = our_products[our_products['price_tier'] == tier]
        if len(tier_products) > 0:
            top_brands = tier_products['brandName'].value_counts().head(3)
            print(f"\n  {tier.upper()}:")
            for brand, count in top_brands.items():
                print(f"    - {brand}: {count} products")
    
    return our_products

def load_m5_data():
    """Load M5 analysis results"""
    print("\n=== Loading M5 Analysis Data ===")
    
    # Load unique M5 items
    m5_items = pd.read_csv(DATA_DIR / 'm5_unique_food_items.csv')
    print(f"✓ Loaded {len(m5_items)} unique M5 FOOD items")
    
    # Load item popularity
    m5_popularity = pd.read_csv(DATA_DIR / 'm5_item_popularity.csv')
    print(f"✓ Loaded popularity data for {len(m5_popularity)} items")
    
    # Load price statistics
    m5_prices = pd.read_csv(DATA_DIR / 'm5_price_stats_gbp.csv')
    print(f"✓ Loaded price statistics for {len(m5_prices)} items")
    
    # Merge all M5 data
    m5_data = m5_items.merge(m5_popularity, on='item_id', how='left')
    m5_data = m5_data.merge(m5_prices, on='item_id', how='left')
    
    return m5_data

def create_weighted_mapping(m5_data, our_products):
    """Create mapping with weighted distribution based on popularity"""
    print("\n=== Creating Product Mapping ===")
    
    # Shopping basket distribution (realistic patterns)
    # Most purchases are budget/standard items
    tier_weights = {
        'budget': 0.40,
        'standard': 0.35,
        'premium': 0.20,
        'luxury': 0.05
    }
    
    print("\nTarget distribution for mapping:")
    for tier, weight in tier_weights.items():
        print(f"  - {tier}: {weight*100:.0f}%")
    
    # Sort M5 items by popularity (most popular first)
    m5_data_sorted = m5_data.sort_values('total_sold', ascending=False)
    
    # Create mapping
    mapping_records = []
    
    # Map popular M5 items to budget/standard products more often
    for idx, m5_item in m5_data_sorted.iterrows():
        # Determine tier based on popularity rank
        popularity_rank = idx / len(m5_data_sorted)
        
        if popularity_rank < 0.2:  # Top 20% most popular
            # Bias towards budget/standard
            tier_probs = [0.5, 0.35, 0.12, 0.03]  # budget, standard, premium, luxury
        elif popularity_rank < 0.5:  # Next 30%
            # Normal distribution
            tier_probs = [0.4, 0.35, 0.20, 0.05]
        else:  # Bottom 50%
            # Can be any tier
            tier_probs = [0.35, 0.35, 0.25, 0.05]
        
        # Select tier
        tier = np.random.choice(['budget', 'standard', 'premium', 'luxury'], p=tier_probs)
        
        # Get products from selected tier
        tier_products = our_products[our_products['price_tier'] == tier]
        
        if len(tier_products) > 0:
            # For popular items, pick from popular products in tier
            # For less popular items, pick randomly
            if popularity_rank < 0.3 and len(tier_products) > 10:
                # Pick from top half of tier
                selected_product = tier_products.iloc[:len(tier_products)//2].sample(1).iloc[0]
            else:
                # Random selection from tier
                selected_product = tier_products.sample(1).iloc[0]
            
            mapping_records.append({
                'm5_item_id': m5_item['item_id'],
                'm5_dept': m5_item.get('dept_id', ''),
                'm5_popularity_rank': idx + 1,
                'm5_total_sold': m5_item.get('total_sold', 0),
                'product_id': selected_product['productId'],
                'sku': selected_product['sku'],
                'product_name': selected_product['name'],
                'brand': selected_product['brandName'],
                'price_gbp': selected_product['price_gbp'],
                'price_tier': tier
            })
        else:
            print(f"Warning: No products in {tier} tier for {m5_item['item_id']}")
    
    mapping_df = pd.DataFrame(mapping_records)
    print(f"\n✓ Created mapping for {len(mapping_df)} items")
    
    return mapping_df

def validate_mapping(mapping_df, our_products):
    """Validate the mapping distribution"""
    print("\n=== Validating Mapping Distribution ===")
    
    # Check price distribution
    price_stats = mapping_df['price_gbp'].describe()
    print("\nMapped products price statistics (GBP):")
    print(f"  - Count: {price_stats['count']:.0f}")
    print(f"  - Mean: £{price_stats['mean']:.2f}")
    print(f"  - Std: £{price_stats['std']:.2f}")
    print(f"  - Min: £{price_stats['min']:.2f}")
    print(f"  - 25%: £{price_stats['25%']:.2f}")
    print(f"  - 50%: £{price_stats['50%']:.2f}")
    print(f"  - 75%: £{price_stats['75%']:.2f}")
    print(f"  - Max: £{price_stats['max']:.2f}")
    
    # Check tier distribution
    tier_dist = mapping_df['price_tier'].value_counts()
    tier_pct = mapping_df['price_tier'].value_counts(normalize=True) * 100
    
    print("\nActual tier distribution in mapping:")
    for tier in ['budget', 'standard', 'premium', 'luxury']:
        count = tier_dist.get(tier, 0)
        pct = tier_pct.get(tier, 0)
        print(f"  - {tier}: {count} items ({pct:.1f}%)")
    
    # Check product reuse
    product_usage = mapping_df['product_id'].value_counts()
    print(f"\nProduct reuse statistics:")
    print(f"  - Unique products used: {len(product_usage)} out of {len(our_products)}")
    print(f"  - Average mappings per product: {product_usage.mean():.1f}")
    print(f"  - Max mappings for one product: {product_usage.max()}")
    
    # Check popular M5 items mapping
    print("\nTop 10 M5 items mapping:")
    top_mappings = mapping_df.nsmallest(10, 'm5_popularity_rank')[
        ['m5_item_id', 'm5_total_sold', 'product_name', 'price_tier', 'price_gbp']
    ]
    for _, row in top_mappings.iterrows():
        print(f"  - {row['m5_item_id']} ({row['m5_total_sold']:.0f} sold) → "
              f"{row['product_name'][:40]}... ({row['price_tier']}, £{row['price_gbp']:.2f})")
    
    return True

def create_visualizations(mapping_df, our_products):
    """Create visualization of the mapping"""
    print("\n=== Creating Visualizations ===")
    
    plt.style.use('seaborn-v0_8-darkgrid')
    fig, axes = plt.subplots(2, 2, figsize=(15, 10))
    
    # 1. Price distribution comparison
    ax1 = axes[0, 0]
    ax1.hist(our_products['price_gbp'], bins=30, alpha=0.5, label='All Products', edgecolor='black')
    ax1.hist(mapping_df['price_gbp'], bins=30, alpha=0.5, label='Mapped Products', edgecolor='black')
    ax1.set_xlabel('Price (GBP)')
    ax1.set_ylabel('Count')
    ax1.set_title('Price Distribution: All Products vs Mapped')
    ax1.legend()
    
    # 2. Tier distribution
    ax2 = axes[0, 1]
    tier_counts = mapping_df['price_tier'].value_counts()
    tier_counts.plot(kind='bar', ax=ax2)
    ax2.set_title('Mapped Products by Price Tier')
    ax2.set_xlabel('Price Tier')
    ax2.set_ylabel('Count')
    ax2.tick_params(axis='x', rotation=45)
    
    # 3. Popularity vs Price scatter
    ax3 = axes[1, 0]
    scatter_data = mapping_df[mapping_df['m5_total_sold'] > 0]
    scatter = ax3.scatter(
        scatter_data['m5_popularity_rank'],
        scatter_data['price_gbp'],
        c=scatter_data['m5_total_sold'],
        cmap='viridis',
        alpha=0.6
    )
    ax3.set_xlabel('M5 Popularity Rank')
    ax3.set_ylabel('Mapped Product Price (£)')
    ax3.set_title('M5 Popularity vs Mapped Product Price')
    plt.colorbar(scatter, ax=ax3, label='M5 Total Sold')
    
    # 4. Product usage distribution
    ax4 = axes[1, 1]
    product_usage = mapping_df['product_id'].value_counts()
    usage_counts = product_usage.value_counts().sort_index()
    ax4.bar(usage_counts.index, usage_counts.values)
    ax4.set_xlabel('Number of M5 Items Mapped to Product')
    ax4.set_ylabel('Number of Products')
    ax4.set_title('Product Reuse Distribution')
    ax4.set_xticks(range(1, min(11, usage_counts.index.max() + 1)))
    
    plt.tight_layout()
    plt.savefig(DATA_DIR / 'product_mapping_distribution.png', dpi=300, bbox_inches='tight')
    print("✓ Visualizations saved to product_mapping_distribution.png")
    plt.close()

def save_mapping_files(mapping_df):
    """Save mapping data for use in order generation"""
    print("\n=== Saving Mapping Files ===")
    
    # Save full mapping CSV
    mapping_df.to_csv(DATA_DIR / 'm5_product_mapping.csv', index=False)
    print(f"✓ Saved mapping to m5_product_mapping.csv ({len(mapping_df)} rows)")
    
    # Create lookup dictionaries
    m5_to_product = dict(zip(mapping_df['m5_item_id'], mapping_df['product_id']))
    product_to_m5 = {}
    for m5_id, prod_id in m5_to_product.items():
        if prod_id not in product_to_m5:
            product_to_m5[prod_id] = []
        product_to_m5[prod_id].append(m5_id)
    
    # Save lookup pickle
    lookup_data = {
        'm5_to_product': m5_to_product,
        'product_to_m5': product_to_m5,
        'mapping_df': mapping_df,
        'mapping_timestamp': datetime.now().isoformat()
    }
    
    with open(DATA_DIR / 'm5_product_lookup.pkl', 'wb') as f:
        pickle.dump(lookup_data, f)
    print("✓ Saved lookup dictionaries to m5_product_lookup.pkl")
    
    # Save summary statistics
    summary = {
        'total_mappings': len(mapping_df),
        'unique_products_used': mapping_df['product_id'].nunique(),
        'price_stats': {
            'mean': float(mapping_df['price_gbp'].mean()),
            'min': float(mapping_df['price_gbp'].min()),
            'max': float(mapping_df['price_gbp'].max()),
            'median': float(mapping_df['price_gbp'].median())
        },
        'tier_distribution': mapping_df['price_tier'].value_counts().to_dict(),
        'avg_mappings_per_product': float(len(mapping_df) / mapping_df['product_id'].nunique())
    }
    
    with open(DATA_DIR / 'mapping_summary.json', 'w') as f:
        json.dump(summary, f, indent=2)
    print("✓ Saved mapping summary to mapping_summary.json")

def main():
    """Main mapping function"""
    print("=" * 60)
    print("Product Mapping: M5 Items to Our Products")
    print("=" * 60)
    print(f"Execution time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Load our products
    our_products = load_our_products()
    
    # Analyze product distribution
    our_products = analyze_product_distribution(our_products)
    
    # Load M5 data
    m5_data = load_m5_data()
    
    # Create weighted mapping
    mapping_df = create_weighted_mapping(m5_data, our_products)
    
    # Validate mapping
    validate_mapping(mapping_df, our_products)
    
    # Create visualizations
    create_visualizations(mapping_df, our_products)
    
    # Save mapping files
    save_mapping_files(mapping_df)
    
    print("\n" + "=" * 60)
    print("✓ PRODUCT MAPPING COMPLETE")
    print("\nSummary:")
    print(f"  - Mapped {len(mapping_df)} M5 items to {mapping_df['product_id'].nunique()} products")
    print(f"  - Average price of mapped products: £{mapping_df['price_gbp'].mean():.2f}")
    print(f"  - Price range: £{mapping_df['price_gbp'].min():.2f} - £{mapping_df['price_gbp'].max():.2f}")
    print("\nNext step: Run script 04_customer_distribution.py")

if __name__ == "__main__":
    main()