#!/usr/bin/env python3
"""
Check current forecast state before purging
"""

import duckdb
from colorama import init, Fore, Style

init()

def main():
    print(f"\n{Fore.CYAN}Current Forecast State{Style.RESET_ALL}")
    print("="*60)
    
    try:
        conn = duckdb.connect('../data/grocery_final.db', read_only=True)
        
        # Check forecast summary
        summary = conn.execute("""
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT forecast_id) as unique_forecast_ids,
                COUNT(DISTINCT productId) as unique_products,
                MIN(forecast_date) as min_forecast_date,
                MAX(forecast_date) as max_forecast_date,
                MIN(target_date) as min_target_date,
                MAX(target_date) as max_target_date
            FROM forecasts
        """).fetchone()
        
        print(f"Total forecast records: {summary[0]:,}")
        print(f"Unique forecast IDs: {summary[1]:,}")
        print(f"Unique products: {summary[2]:,}")
        print(f"Forecast date range: {summary[3]} to {summary[4]}")
        print(f"Target date range: {summary[5]} to {summary[6]}")
        
        # Check stock summary
        stock_summary = conn.execute("""
            SELECT 
                COUNT(DISTINCT productId) as products_in_stock,
                SUM(quantity_in_stock) as total_stock_quantity
            FROM stock
            WHERE quantity_in_stock > 0
        """).fetchone()
        
        print(f"\nProducts currently in stock: {stock_summary[0]:,}")
        print(f"Total stock quantity: {stock_summary[1]:,}")
        
        # Sample forecast IDs
        print("\nSample forecast IDs:")
        forecast_ids = conn.execute("""
            SELECT 
                forecast_id,
                MIN(forecast_date) as forecast_date,
                COUNT(DISTINCT productId) as product_count,
                COUNT(*) as record_count
            FROM forecasts
            GROUP BY forecast_id
            ORDER BY MIN(forecast_date) DESC
            LIMIT 5
        """).fetchall()
        
        for fid, date, prod_count, rec_count in forecast_ids:
            print(f"  {fid}: {date} ({prod_count} products, {rec_count} records)")
        
        conn.close()
        
    except Exception as e:
        print(f"{Fore.RED}Error: {str(e)}{Style.RESET_ALL}")

if __name__ == "__main__":
    main()