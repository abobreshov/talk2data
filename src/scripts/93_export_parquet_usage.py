#!/usr/bin/env python3
"""
Example usage of Parquet files
This script demonstrates how to work with the exported Parquet files
"""

import duckdb
import pandas as pd
from pathlib import Path
import json

# Setup paths
DATA_DIR = Path(__file__).parent.parent / 'data'
PARQUET_DIR = DATA_DIR / 'parquet'

def example_duckdb_queries():
    """Demonstrate DuckDB queries on Parquet files"""
    print("=" * 60)
    print("DuckDB Parquet Query Examples")
    print("=" * 60)
    
    # Create in-memory DuckDB connection
    conn = duckdb.connect(':memory:')
    
    # Example 1: Direct query on parquet file
    print("\n1. Top 5 products by price:")
    result = conn.execute(f"""
        SELECT productId, name, brandName, price_gbp
        FROM read_parquet('{PARQUET_DIR}/products.parquet')
        ORDER BY price_gbp DESC
        LIMIT 5
    """).df()
    print(result)
    
    # Example 2: Join across multiple parquet files
    print("\n2. Top 5 customers by lifetime value:")
    result = conn.execute(f"""
        SELECT 
            c.customerId,
            c.first_name || ' ' || c.last_name as customer_name,
            c.city,
            ca.total_orders,
            ca.lifetime_value
        FROM read_parquet('{PARQUET_DIR}/customers.parquet') c
        JOIN read_parquet('{PARQUET_DIR}/customer_analytics_view.parquet') ca
            ON c.customerId = ca.customerId
        ORDER BY ca.lifetime_value DESC
        LIMIT 5
    """).df()
    print(result)
    
    # Example 3: Aggregate query
    print("\n3. Sales by month:")
    result = conn.execute(f"""
        SELECT 
            DATE_TRUNC('month', saleDate) as month,
            COUNT(*) as sales_count,
            SUM(unitPrice * quantity) as revenue
        FROM read_parquet('{PARQUET_DIR}/sales.parquet')
        GROUP BY month
        ORDER BY month
        LIMIT 6
    """).df()
    print(result)
    
    # Example 4: Create a view from parquet files
    print("\n4. Creating views from Parquet files:")
    conn.execute(f"""
        CREATE VIEW products_parquet AS 
        SELECT * FROM read_parquet('{PARQUET_DIR}/products.parquet')
    """)
    
    conn.execute(f"""
        CREATE VIEW product_performance_parquet AS 
        SELECT * FROM read_parquet('{PARQUET_DIR}/product_performance_view.parquet')
    """)
    
    # Query the views
    result = conn.execute("""
        SELECT 
            p.name,
            p.brandName,
            pp.units_sold,
            pp.revenue
        FROM products_parquet p
        JOIN product_performance_parquet pp ON p.productId = pp.productId
        WHERE pp.units_sold > 0
        ORDER BY pp.revenue DESC
        LIMIT 5
    """).df()
    print("Top 5 products by revenue:")
    print(result)
    
    conn.close()

def example_pandas_usage():
    """Demonstrate pandas usage with Parquet files"""
    print("\n" + "=" * 60)
    print("Pandas Parquet Examples")
    print("=" * 60)
    
    # Example 1: Read parquet file into pandas
    print("\n1. Loading products into pandas DataFrame:")
    products_df = pd.read_parquet(PARQUET_DIR / 'products.parquet')
    print(f"Shape: {products_df.shape}")
    print(f"Columns: {products_df.columns.tolist()}")
    print("\nFirst 3 rows:")
    print(products_df.head(3))
    
    # Example 2: Filter and analyze
    print("\n2. Product price analysis:")
    price_stats = products_df['price_gbp'].describe()
    print(price_stats)
    
    # Example 3: Memory efficiency
    print("\n3. Memory usage comparison:")
    # Parquet file size
    parquet_size_mb = (PARQUET_DIR / 'sales.parquet').stat().st_size / (1024 * 1024)
    
    # Load a sample to check memory
    sales_sample = pd.read_parquet(
        PARQUET_DIR / 'sales.parquet',
        columns=['saleId', 'productId', 'quantity', 'unitPrice']
    )
    memory_usage_mb = sales_sample.memory_usage(deep=True).sum() / (1024 * 1024)
    
    print(f"Parquet file size: {parquet_size_mb:.2f} MB")
    print(f"DataFrame memory usage: {memory_usage_mb:.2f} MB")
    print(f"Compression ratio: {memory_usage_mb/parquet_size_mb:.2f}x")

def example_export_info():
    """Show export summary information"""
    print("\n" + "=" * 60)
    print("Export Summary")
    print("=" * 60)
    
    with open(PARQUET_DIR / 'export_summary.json', 'r') as f:
        summary = json.load(f)
    
    print(f"\nExport timestamp: {summary['export_timestamp']}")
    print(f"Total files: {summary['total_files']}")
    
    print("\nFile sizes:")
    total_size = 0
    for name, info in summary['exports'].items():
        size_mb = info['file_size_mb']
        total_size += size_mb
        print(f"  {name:30} {size_mb:8.2f} MB  ({info['rows']:,} rows)")
    
    print(f"\nTotal size: {total_size:.2f} MB")

def main():
    """Main function"""
    print("Parquet File Usage Examples")
    print("=" * 60)
    
    # Check if parquet files exist
    if not PARQUET_DIR.exists():
        print("✗ Error: Parquet directory not found")
        print("  Please run 12_export_to_parquet.py first")
        return
    
    # Run examples
    example_duckdb_queries()
    example_pandas_usage()
    example_export_info()
    
    print("\n" + "=" * 60)
    print("✓ Examples completed successfully")

if __name__ == "__main__":
    main()