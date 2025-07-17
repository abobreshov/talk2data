#!/usr/bin/env python3
"""
Generate stock levels based on historical sales patterns.

Stock generation rules:
1. Warehouse capacity = max daily sales * 2
2. Multiple SKUs per product based on different expiration dates
3. Only include products with sales in the last 30 days
4. Stock distributed across different expiration batches
"""

import duckdb
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import json
from colorama import init, Fore, Style

init(autoreset=True)

def generate_stock_levels():
    """Generate current stock levels based on sales patterns."""
    print(f"\n{Fore.CYAN}{'='*60}")
    print(f"{Fore.CYAN}Generating Stock Levels")
    print(f"{Fore.CYAN}{'='*60}\n")
    
    # Connect to database
    db_path = Path(__file__).parent.parent / "data" / "grocery_final.db"
    conn = duckdb.connect(str(db_path))
    
    # Get latest sale date
    latest_date = conn.execute("SELECT MAX(saleDate) FROM sales").fetchone()[0]
    print(f"{Fore.YELLOW}Latest sale date: {latest_date}")
    
    # Get products sold in last 30 days with their max daily sales
    print(f"\n{Fore.YELLOW}Analyzing sales from last 30 days...")
    
    product_sales_analysis = conn.execute("""
        WITH daily_sales AS (
            SELECT 
                productId,
                DATE_TRUNC('day', saleDate) as sale_day,
                SUM(quantity) as daily_quantity
            FROM sales
            WHERE saleDate >= ? - INTERVAL '30 days'
            GROUP BY productId, sale_day
        ),
        product_stats AS (
            SELECT 
                productId,
                MAX(daily_quantity) as max_daily_sales,
                AVG(daily_quantity) as avg_daily_sales,
                COUNT(DISTINCT sale_day) as days_sold
            FROM daily_sales
            GROUP BY productId
        )
        SELECT 
            ps.productId,
            p.name as product_name,
            p.category,
            p.subcategory,
            ps.max_daily_sales,
            ps.avg_daily_sales,
            ps.days_sold,
            ppr.shelf_life_days,
            ppr.temperature_sensitive,
            ppr.requires_refrigeration,
            -- Warehouse capacity = max daily sales * 2
            ps.max_daily_sales * 2 as warehouse_capacity
        FROM product_stats ps
        JOIN products p ON ps.productId = p.productId
        LEFT JOIN product_purge_reference ppr 
            ON p.category = ppr.category 
            AND (p.subcategory = ppr.subcategory OR (p.subcategory IS NULL AND ppr.subcategory IS NULL))
        ORDER BY ps.max_daily_sales DESC
    """, [latest_date]).df()
    
    print(f"{Fore.GREEN}✓ Found {len(product_sales_analysis)} products with sales in last 30 days")
    
    # Get SKUs for these products
    product_ids = product_sales_analysis['productId'].tolist()
    
    print(f"\n{Fore.YELLOW}Loading SKU mappings...")
    sku_data = conn.execute("""
        SELECT 
            ps.productId,
            ps.sku
        FROM product_skus ps
        WHERE ps.productId IN (SELECT UNNEST(?))
        ORDER BY ps.productId, ps.sku
    """, [product_ids]).df()
    
    print(f"{Fore.GREEN}✓ Found {len(sku_data)} SKUs for active products")
    
    # Generate stock records
    print(f"\n{Fore.YELLOW}Generating stock records...")
    stock_records = []
    stock_date = datetime.now().date()
    
    for _, product in product_sales_analysis.iterrows():
        product_id = product['productId']
        warehouse_capacity = int(product['warehouse_capacity'])
        shelf_life_days = product['shelf_life_days'] or 30  # Default 30 days if null
        
        # Get SKUs for this product
        product_skus = sku_data[sku_data['productId'] == product_id]
        
        if len(product_skus) == 0:
            continue
            
        # Distribute stock across SKUs with different expiration dates
        # Create 3-5 batches with different expiration dates
        num_batches = min(len(product_skus), np.random.choice([3, 4, 5]))
        
        # Calculate stock per batch (with some randomness)
        base_stock_per_batch = warehouse_capacity // num_batches
        
        for i in range(num_batches):
            if i >= len(product_skus):
                break
                
            sku_row = product_skus.iloc[i]
            
            # Calculate expiration date based on when batch was received
            # Newer batches have longer expiration
            days_since_received = (num_batches - i - 1) * (shelf_life_days // num_batches)
            expiration_date = stock_date + timedelta(days=shelf_life_days - days_since_received)
            
            # Add some variation to stock levels
            stock_variation = np.random.uniform(0.8, 1.2)
            quantity_in_stock = int(base_stock_per_batch * stock_variation)
            
            # Some batches might be partially depleted
            if i == 0:  # Oldest batch
                quantity_in_stock = int(quantity_in_stock * np.random.uniform(0.3, 0.7))
            elif i == 1:  # Second oldest
                quantity_in_stock = int(quantity_in_stock * np.random.uniform(0.6, 0.9))
            
            # Calculate received date
            received_date = stock_date - timedelta(days=days_since_received)
            
            stock_records.append({
                'sku': sku_row['sku'],
                'productId': product_id,
                'quantity_in_stock': quantity_in_stock,
                'expiration_date': expiration_date.strftime('%Y-%m-%d'),
                'batch_number': f"BATCH-{product_id[:8]}-{expiration_date.strftime('%Y%m%d')}",
                'received_date': received_date.strftime('%Y-%m-%d'),
                'temperature_zone': 'CHILLED' if product['requires_refrigeration'] else 'AMBIENT',
                'stock_status': 'AVAILABLE'
            })
    
    stock_df = pd.DataFrame(stock_records)
    
    # Create stock table
    print(f"\n{Fore.CYAN}Creating stock table...")
    
    conn.execute("DROP TABLE IF EXISTS stock")
    conn.execute("DROP SEQUENCE IF EXISTS stock_seq")
    
    conn.execute("""
        CREATE SEQUENCE stock_seq;
        CREATE TABLE stock (
            stock_id INTEGER PRIMARY KEY DEFAULT nextval('stock_seq'),
            sku VARCHAR NOT NULL,
            productId VARCHAR NOT NULL,
            quantity_in_stock INTEGER NOT NULL,
            expiration_date DATE NOT NULL,
            batch_number VARCHAR NOT NULL,
            received_date DATE NOT NULL,
            temperature_zone VARCHAR NOT NULL,
            stock_status VARCHAR NOT NULL,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (productId) REFERENCES products(productId)
        )
    """)
    
    # Insert stock data
    conn.execute("""
        INSERT INTO stock 
        (sku, productId, quantity_in_stock, expiration_date, 
         batch_number, received_date, temperature_zone, stock_status)
        SELECT * FROM stock_df
    """)
    
    # Create indexes
    conn.execute("CREATE INDEX idx_stock_sku ON stock(sku)")
    conn.execute("CREATE INDEX idx_stock_product ON stock(productId)")
    conn.execute("CREATE INDEX idx_stock_expiration ON stock(expiration_date)")
    conn.execute("CREATE INDEX idx_stock_status ON stock(stock_status)")
    
    # Add unique constraint on SKU + expiration_date (same SKU can't have same expiration)
    conn.execute("CREATE UNIQUE INDEX idx_stock_sku_expiration ON stock(sku, expiration_date)")
    
    # Create useful views
    conn.execute("""
        CREATE OR REPLACE VIEW stock_summary AS
        SELECT 
            s.productId,
            p.name as product_name,
            p.category,
            p.subcategory,
            COUNT(DISTINCT s.sku) as num_skus,
            COUNT(DISTINCT s.batch_number) as num_batches,
            SUM(s.quantity_in_stock) as total_stock,
            MIN(s.expiration_date) as earliest_expiration,
            MAX(s.expiration_date) as latest_expiration,
            SUM(CASE WHEN s.expiration_date <= CURRENT_DATE + INTERVAL '7 days' 
                THEN s.quantity_in_stock ELSE 0 END) as expiring_soon_qty
        FROM stock s
        JOIN products p ON s.productId = p.productId
        WHERE s.stock_status = 'AVAILABLE'
        GROUP BY s.productId, p.name, p.category, p.subcategory
    """)
    
    conn.execute("""
        CREATE OR REPLACE VIEW stock_by_zone AS
        SELECT 
            temperature_zone,
            COUNT(DISTINCT productId) as num_products,
            COUNT(DISTINCT sku) as num_skus,
            SUM(quantity_in_stock) as total_items,
            COUNT(DISTINCT batch_number) as num_batches
        FROM stock
        WHERE stock_status = 'AVAILABLE'
        GROUP BY temperature_zone
        ORDER BY temperature_zone
    """)
    
    print(f"{Fore.GREEN}✓ Created views: stock_summary, stock_by_zone")
    
    # Show summary statistics
    total_stock_records = conn.execute("SELECT COUNT(*) FROM stock").fetchone()[0]
    print(f"\n{Fore.GREEN}✓ Created {total_stock_records:,} stock records")
    
    # Show distribution
    print(f"\n{Fore.YELLOW}Stock Distribution Summary:")
    location_summary = conn.execute("""
        SELECT 
            temperature_zone,
            COUNT(DISTINCT productId) as products,
            COUNT(DISTINCT sku) as skus,
            SUM(quantity_in_stock) as total_items,
            ROUND(AVG(quantity_in_stock), 1) as avg_items_per_sku
        FROM stock
        GROUP BY temperature_zone
        ORDER BY temperature_zone
    """).df()
    
    print(location_summary.to_string(index=False))
    
    # Show expiration summary
    print(f"\n{Fore.YELLOW}Expiration Timeline:")
    expiration_summary = conn.execute("""
        SELECT 
            CASE 
                WHEN expiration_date <= CURRENT_DATE THEN 'Expired'
                WHEN expiration_date <= CURRENT_DATE + INTERVAL '3 days' THEN '1-3 days'
                WHEN expiration_date <= CURRENT_DATE + INTERVAL '7 days' THEN '4-7 days'
                WHEN expiration_date <= CURRENT_DATE + INTERVAL '14 days' THEN '8-14 days'
                WHEN expiration_date <= CURRENT_DATE + INTERVAL '30 days' THEN '15-30 days'
                ELSE '30+ days'
            END as expiration_range,
            COUNT(*) as num_batches,
            SUM(quantity_in_stock) as total_items
        FROM stock
        WHERE stock_status = 'AVAILABLE'
        GROUP BY expiration_range
        ORDER BY 
            CASE expiration_range
                WHEN 'Expired' THEN 1
                WHEN '1-3 days' THEN 2
                WHEN '4-7 days' THEN 3
                WHEN '8-14 days' THEN 4
                WHEN '15-30 days' THEN 5
                ELSE 6
            END
    """).df()
    
    print(expiration_summary.to_string(index=False))
    
    # Show sample stock records
    print(f"\n{Fore.YELLOW}Sample Stock Records:")
    sample_stock = conn.execute("""
        SELECT 
            p.name as product,
            s.sku,
            s.quantity_in_stock as qty,
            s.expiration_date as expires,
            s.batch_number as batch,
            s.temperature_zone as zone
        FROM stock s
        JOIN products p ON s.productId = p.productId
        WHERE s.productId IN (
            SELECT productId 
            FROM stock_summary 
            ORDER BY total_stock DESC 
            LIMIT 3
        )
        ORDER BY p.name, s.expiration_date
        LIMIT 15
    """).df()
    
    print(sample_stock.to_string(index=False))
    
    # Save summary
    summary_data = {
        'total_stock_records': total_stock_records,
        'unique_products': len(product_sales_analysis),
        'total_skus': len(stock_df['sku'].unique()),
        'total_quantity': int(stock_df['quantity_in_stock'].sum()),
        'temperature_zones': location_summary.to_dict('records'),
        'generation_date': datetime.now().isoformat(),
        'sales_analysis_period': '30 days',
        'warehouse_capacity_formula': 'max_daily_sales * 2'
    }
    
    summary_path = Path(__file__).parent.parent / 'data' / 'stock_generation_summary.json'
    with open(summary_path, 'w') as f:
        json.dump(summary_data, f, indent=2)
    
    print(f"\n{Fore.GREEN}✓ Summary saved to stock_generation_summary.json")
    
    conn.close()
    print(f"\n{Fore.CYAN}{'='*60}")
    print(f"{Fore.GREEN}✓ Stock generation complete!")
    print(f"{Fore.CYAN}{'='*60}\n")

if __name__ == "__main__":
    generate_stock_levels()