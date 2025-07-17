#!/usr/bin/env python3
"""
Task 12: M5 Dataset Loading and Analysis
This script loads and analyzes the M5 Walmart dataset to understand its structure
and prepare for order synthesis.
"""

import os
import sys
import pandas as pd
import numpy as np
import duckdb
from pathlib import Path
from datetime import datetime
import matplotlib.pyplot as plt
import seaborn as sns
import pickle
from datasetsforecast.m5 import M5

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))
sys.path.append(str(Path(__file__).parent))

# Load configuration from environment
from utilities.config import DATA_DIR, PRODUCTS_DB, ORDERS_DB, CUSTOMERS_CSV, get_config
config = get_config()  # For compatibility

NOTEBOOKS_DIR = project_root / "src" / "notebooks"

def load_m5_dataset():
    """Load M5 dataset and return dataframes"""
    print("=== Loading M5 Dataset ===")
    print("This may take a moment...")
    
    try:
        # Load M5 data
        train_df, X_df, S_df = M5.load(directory=str(NOTEBOOKS_DIR / "data"))
        
        print(f"✓ M5 dataset loaded successfully")
        print(f"  - train_df shape: {train_df.shape}")
        print(f"  - X_df shape: {X_df.shape}")
        print(f"  - S_df shape: {S_df.shape}")
        
        return train_df, X_df, S_df
    
    except Exception as e:
        print(f"✗ Error loading M5 dataset: {e}")
        sys.exit(1)

def analyze_dataset_structure(train_df, X_df, S_df):
    """Analyze the structure of M5 dataset"""
    print("\n=== Dataset Structure Analysis ===")
    
    # Train DataFrame
    print("\n1. Train DataFrame (train_df):")
    print(f"   - Shape: {train_df.shape}")
    print(f"   - Columns: {train_df.columns.tolist()}")
    print(f"   - Date range: {train_df['ds'].min()} to {train_df['ds'].max()}")
    print(f"   - Unique products: {train_df['unique_id'].nunique()}")
    print("\n   Sample data:")
    print(train_df.head())
    
    # Exogenous DataFrame
    print("\n2. Exogenous DataFrame (X_df):")
    print(f"   - Shape: {X_df.shape}")
    print(f"   - Columns: {X_df.columns.tolist()}")
    print("\n   Sample data:")
    print(X_df.head())
    
    # Static DataFrame
    print("\n3. Static DataFrame (S_df):")
    print(f"   - Shape: {S_df.shape}")
    print(f"   - Columns: {S_df.columns.tolist()}")
    print(f"   - Categories: {S_df['cat_id'].unique()}")
    print("\n   Sample data:")
    print(S_df.head())

def analyze_foods_category(train_df, X_df, S_df):
    """Focus analysis on FOODS category"""
    print("\n=== FOODS Category Analysis ===")
    
    # Get FOODS items
    foods_items_df = S_df[S_df['cat_id'] == 'FOODS'].copy()
    foods_unique_ids = foods_items_df['unique_id'].unique()
    
    print(f"\nFOODS Category Overview:")
    print(f"  - Unique FOODS items: {len(foods_items_df['item_id'].unique())}")
    print(f"  - FOODS product-store combinations: {len(foods_unique_ids)}")
    print(f"  - Stores: {foods_items_df['store_id'].unique()}")
    print(f"  - States: {foods_items_df['state_id'].unique()}")
    
    # Filter train data for FOODS
    foods_train = train_df[train_df['unique_id'].isin(foods_unique_ids)].copy()
    foods_x = X_df[X_df['unique_id'].isin(foods_unique_ids)].copy()
    
    print(f"\nFOODS Sales Data:")
    print(f"  - Total records: {len(foods_train):,}")
    print(f"  - Date range: {foods_train['ds'].min()} to {foods_train['ds'].max()}")
    
    # Sales statistics
    sales_stats = foods_train['y'].describe()
    print(f"\nSales quantity statistics:")
    for stat, value in sales_stats.items():
        print(f"  - {stat}: {value:.2f}")
    
    # Extract unique FOOD items (without store suffix)
    foods_items_df['item_id_clean'] = foods_items_df['item_id']
    unique_food_items = foods_items_df['item_id_clean'].unique()
    
    print(f"\nUnique FOOD products (across all stores): {len(unique_food_items)}")
    print("\nSample FOOD items:")
    for item in unique_food_items[:10]:
        print(f"  - {item}")
    
    return foods_train, foods_x, foods_items_df

def analyze_sales_patterns(foods_train, foods_x):
    """Analyze sales patterns to understand M5 data structure"""
    print("\n=== Sales Pattern Analysis ===")
    
    # Daily aggregation
    daily_sales = foods_train.groupby('ds').agg({
        'y': ['sum', 'mean', 'count']
    }).reset_index()
    daily_sales.columns = ['date', 'total_quantity', 'avg_quantity', 'transactions']
    
    print(f"\nDaily sales statistics:")
    print(f"  - Average daily quantity sold: {daily_sales['total_quantity'].mean():.0f}")
    print(f"  - Average transactions per day: {daily_sales['transactions'].mean():.0f}")
    
    # Analyze item popularity for realistic order generation
    print("\n=== Item Popularity Analysis ===")
    
    positive_sales = foods_train[foods_train['y'] > 0].copy()
    positive_sales['item_id'] = positive_sales['unique_id'].str.rsplit('_', n=2).str[0]
    
    # Get item popularity (total sales across all stores)
    item_popularity = positive_sales.groupby('item_id')['y'].agg(['sum', 'mean', 'count']).reset_index()
    item_popularity.columns = ['item_id', 'total_sold', 'avg_quantity', 'times_sold']
    item_popularity = item_popularity.sort_values('total_sold', ascending=False)
    
    print(f"\nTop 10 most popular items:")
    for _, item in item_popularity.head(10).iterrows():
        print(f"  - {item['item_id']}: {item['total_sold']:.0f} units sold")
    
    # Calculate percentiles for mapping strategy
    popularity_percentiles = item_popularity['total_sold'].quantile([0.25, 0.5, 0.75, 0.9])
    print(f"\nPopularity percentiles:")
    print(f"  - 25th: {popularity_percentiles[0.25]:.0f} units")
    print(f"  - 50th: {popularity_percentiles[0.5]:.0f} units")
    print(f"  - 75th: {popularity_percentiles[0.75]:.0f} units")
    print(f"  - 90th: {popularity_percentiles[0.9]:.0f} units")
    
    # Note about our approach
    print("\n📝 Note: We will create synthetic baskets during order generation")
    print("   Our sales table will be derived from generated orders")
    print("   Sales will be recorded on delivery_date (not order_date)")
    
    return daily_sales, item_popularity

def analyze_price_information(foods_x, foods_items_df):
    """Analyze price information from M5 data"""
    print("\n=== Price Analysis ===")
    
    # Get price information
    prices = foods_x[['unique_id', 'sell_price']].drop_duplicates()
    
    # Add item_id for grouping
    prices['item_id'] = prices['unique_id'].str.rsplit('_', n=2).str[0]
    
    # Price statistics by item
    price_stats = prices.groupby('item_id')['sell_price'].agg(['min', 'max', 'mean']).reset_index()
    
    print(f"\nPrice statistics (USD):")
    overall_stats = price_stats[['min', 'max', 'mean']].describe()
    print(overall_stats)
    
    # Convert to GBP (approximate 1 USD = 0.8 GBP)
    price_stats_gbp = price_stats.copy()
    price_stats_gbp[['min', 'max', 'mean']] = price_stats[['min', 'max', 'mean']] * 0.8
    
    print(f"\nPrice statistics (GBP - converted):")
    overall_stats_gbp = price_stats_gbp[['min', 'max', 'mean']].describe()
    print(overall_stats_gbp)
    
    # Save price mapping
    price_stats_gbp.to_csv(DATA_DIR / 'm5_price_stats_gbp.csv', index=False)
    print(f"\n✓ Price statistics saved to m5_price_stats_gbp.csv")
    
    return price_stats_gbp

def analyze_temporal_patterns(foods_train):
    """Analyze temporal patterns for order generation"""
    print("\n=== Temporal Pattern Analysis ===")
    
    # Add temporal features
    foods_train['date'] = pd.to_datetime(foods_train['ds'])
    foods_train['dow'] = foods_train['date'].dt.dayofweek
    foods_train['month'] = foods_train['date'].dt.month
    foods_train['year'] = foods_train['date'].dt.year
    
    # Day of week patterns
    dow_patterns = foods_train[foods_train['y'] > 0].groupby('dow')['y'].agg(['sum', 'mean', 'count'])
    dow_patterns.index = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
    
    print("\nSales by day of week:")
    print(dow_patterns)
    
    # Monthly patterns
    monthly_patterns = foods_train[foods_train['y'] > 0].groupby('month')['y'].agg(['sum', 'mean', 'count'])
    
    print("\nSales by month:")
    print(monthly_patterns)
    
    return dow_patterns, monthly_patterns

def save_analysis_results(foods_items_df, item_popularity, price_stats_gbp):
    """Save analysis results for next steps"""
    print("\n=== Saving Analysis Results ===")
    
    analysis_results = {
        'analysis_timestamp': datetime.now().isoformat(),
        'foods_items_count': len(foods_items_df['item_id'].unique()),
        'total_items_analyzed': len(item_popularity),
        'price_range_gbp': {
            'min': float(price_stats_gbp['mean'].min()),
            'max': float(price_stats_gbp['mean'].max()),
            'avg': float(price_stats_gbp['mean'].mean())
        },
        'notes': 'Sales table will be generated from orders on delivery_date'
    }
    
    # Save to JSON
    with open(DATA_DIR / 'm5_analysis_results.json', 'w') as f:
        json.dump(analysis_results, f, indent=2)
    
    # Save item popularity for mapping strategy
    item_popularity.to_csv(DATA_DIR / 'm5_item_popularity.csv', index=False)
    
    # Save unique FOOD items
    unique_items = foods_items_df[['item_id', 'dept_id']].drop_duplicates()
    unique_items.to_csv(DATA_DIR / 'm5_unique_food_items.csv', index=False)
    
    print(f"✓ Analysis results saved")
    print(f"  - m5_analysis_results.json")
    print(f"  - m5_item_popularity.csv ({len(item_popularity)} items)")
    print(f"  - m5_unique_food_items.csv ({len(unique_items)} items)")
    print(f"  - m5_price_stats_gbp.csv")

def create_visualizations(daily_sales, item_popularity, dow_patterns, price_stats_gbp):
    """Create and save visualization plots"""
    print("\n=== Creating Visualizations ===")
    
    plt.style.use('seaborn-v0_8-darkgrid')
    fig, axes = plt.subplots(2, 2, figsize=(15, 10))
    
    # 1. Daily sales trend
    ax1 = axes[0, 0]
    daily_sales_sample = daily_sales.iloc[::30]  # Sample every 30 days for clarity
    ax1.plot(daily_sales_sample['date'], daily_sales_sample['total_quantity'])
    ax1.set_title('Daily Sales Trend (FOODS Category)')
    ax1.set_xlabel('Date')
    ax1.set_ylabel('Total Quantity Sold')
    ax1.tick_params(axis='x', rotation=45)
    
    # 2. Item popularity distribution
    ax2 = axes[0, 1]
    item_popularity['total_sold'].hist(bins=50, ax=ax2, edgecolor='black')
    ax2.set_title('Item Popularity Distribution')
    ax2.set_xlabel('Total Units Sold')
    ax2.set_ylabel('Number of Items')
    ax2.set_yscale('log')  # Log scale for better visibility
    
    # 3. Day of week patterns
    ax3 = axes[1, 0]
    dow_patterns['sum'].plot(kind='bar', ax=ax3)
    ax3.set_title('Sales by Day of Week')
    ax3.set_xlabel('Day')
    ax3.set_ylabel('Total Quantity Sold')
    ax3.tick_params(axis='x', rotation=45)
    
    # 4. Price distribution
    ax4 = axes[1, 1]
    price_stats_gbp['mean'].hist(bins=30, ax=ax4, edgecolor='black')
    ax4.axvline(price_stats_gbp['mean'].mean(), color='red', linestyle='--',
                label=f'Mean: £{price_stats_gbp["mean"].mean():.2f}')
    ax4.set_title('Product Price Distribution (GBP)')
    ax4.set_xlabel('Price (£)')
    ax4.set_ylabel('Number of Products')
    ax4.legend()
    
    plt.tight_layout()
    plt.savefig(DATA_DIR / 'm5_analysis_plots.png', dpi=300, bbox_inches='tight')
    print("✓ Visualizations saved to m5_analysis_plots.png")
    plt.close()

def main():
    """Main analysis function"""
    print("=" * 60)
    print("M5 Dataset Analysis for Orders Synthesis")
    print("=" * 60)
    print(f"Execution time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Load M5 dataset
    train_df, X_df, S_df = load_m5_dataset()
    
    # Analyze dataset structure
    analyze_dataset_structure(train_df, X_df, S_df)
    
    # Focus on FOODS category
    foods_train, foods_x, foods_items_df = analyze_foods_category(train_df, X_df, S_df)
    
    # Analyze sales patterns
    daily_sales, item_popularity = analyze_sales_patterns(foods_train, foods_x)
    
    # Analyze prices
    price_stats_gbp = analyze_price_information(foods_x, foods_items_df)
    
    # Analyze temporal patterns
    dow_patterns, monthly_patterns = analyze_temporal_patterns(foods_train)
    
    # Create visualizations
    create_visualizations(daily_sales, item_popularity, dow_patterns, price_stats_gbp)
    
    # Save results
    save_analysis_results(foods_items_df, item_popularity, price_stats_gbp)
    
    print("\n" + "=" * 60)
    print("✓ M5 DATASET ANALYSIS COMPLETE")
    print("\nKey findings:")
    print(f"  - {len(foods_items_df['item_id'].unique())} unique FOOD items to map")
    print(f"  - Item popularity data saved for mapping strategy")
    print(f"  - Price range (GBP): £{price_stats_gbp['mean'].min():.2f} - £{price_stats_gbp['mean'].max():.2f}")
    print(f"  - Sales patterns analyzed for realistic order generation")
    print("\n📝 Note: Sales table will be generated from orders")
    print("   Each order item will create a sale record on delivery date")
    print("\nNext step: Run script 03_product_mapping.py")

if __name__ == "__main__":
    main()