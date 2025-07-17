#!/usr/bin/env python3
"""
Purge existing forecasts and regenerate for products in stock.
Uses productId (not SKU) for forecasting.
Historical window: 180 days
Forecast horizon: 7 days
"""

import duckdb
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import uuid
from tqdm import tqdm
from colorama import init, Fore, Style

# Initialize colorama
init()

def print_header(text):
    """Print formatted header"""
    print(f"\n{Fore.CYAN}{'='*60}{Style.RESET_ALL}")
    print(f"{Fore.CYAN}{text}{Style.RESET_ALL}")
    print(f"{Fore.CYAN}{'='*60}{Style.RESET_ALL}")

def get_products_in_stock(conn):
    """Get all products currently in stock"""
    query = """
    SELECT DISTINCT 
        st.productId,
        p.name as product_name,
        p.category,
        SUM(st.quantity_in_stock) as total_stock
    FROM stock st
    JOIN products p ON st.productId = p.productId
    WHERE st.quantity_in_stock > 0
    GROUP BY st.productId, p.name, p.category
    ORDER BY st.productId
    """
    return conn.execute(query).fetchdf()

def get_historical_sales(conn, product_id, days=180):
    """Get historical sales for a product"""
    query = f"""
    SELECT 
        DATE_TRUNC('day', saleDate) as sale_date,
        SUM(quantity) as daily_quantity
    FROM sales
    WHERE productId = $1
    AND saleDate >= CURRENT_DATE - INTERVAL '{days} days'
    AND saleDate < CURRENT_DATE
    GROUP BY DATE_TRUNC('day', saleDate)
    ORDER BY sale_date
    """
    return conn.execute(query, [product_id]).fetchdf()

def calculate_forecast(historical_sales, horizon_days=7):
    """Calculate forecast using historical patterns"""
    if historical_sales.empty:
        # No historical data - return minimal forecast (rounded)
        return pd.DataFrame({
            'day_offset': range(1, horizon_days + 1),
            'predicted_quantity': [1] * horizon_days,
            'confidence_lower': [1] * horizon_days,
            'confidence_upper': [2] * horizon_days
        })
    
    # Calculate day of week patterns
    historical_sales['dow'] = pd.to_datetime(historical_sales['sale_date']).dt.dayofweek
    dow_avg = historical_sales.groupby('dow')['daily_quantity'].agg(['mean', 'std']).reset_index()
    
    # Fill missing days
    for dow in range(7):
        if dow not in dow_avg['dow'].values:
            dow_avg = pd.concat([dow_avg, pd.DataFrame({
                'dow': [dow],
                'mean': [historical_sales['daily_quantity'].mean()],
                'std': [historical_sales['daily_quantity'].std()]
            })], ignore_index=True)
    
    dow_avg = dow_avg.sort_values('dow')
    
    # Generate forecast
    forecast_start = pd.Timestamp.now().date() + timedelta(days=1)
    forecasts = []
    
    for day in range(horizon_days):
        target_date = forecast_start + timedelta(days=day)
        dow = target_date.weekday()
        
        mean_val = dow_avg[dow_avg['dow'] == dow]['mean'].values[0]
        std_val = dow_avg[dow_avg['dow'] == dow]['std'].values[0]
        
        # Handle NaN std
        if pd.isna(std_val) or std_val == 0:
            std_val = mean_val * 0.2  # 20% of mean as default
        
        # Ensure positive values and round to integers
        predicted = max(1, round(mean_val))
        lower = max(1, round(mean_val - 1.96 * std_val))
        upper = max(predicted + 1, round(mean_val + 1.96 * std_val))
        
        forecasts.append({
            'day_offset': day + 1,
            'predicted_quantity': float(predicted),
            'confidence_lower': float(lower),
            'confidence_upper': float(upper)
        })
    
    return pd.DataFrame(forecasts)

def purge_existing_forecasts(conn):
    """Purge all existing forecast records"""
    print_header("Purging Existing Forecasts")
    
    # Get count before purge
    count_before = conn.execute("SELECT COUNT(*) FROM forecasts").fetchone()[0]
    print(f"Forecasts before purge: {count_before:,}")
    
    # Purge all forecasts
    conn.execute("DELETE FROM forecasts")
    
    # Verify purge
    count_after = conn.execute("SELECT COUNT(*) FROM forecasts").fetchone()[0]
    print(f"Forecasts after purge: {count_after:,}")
    print(f"{Fore.GREEN}✓ Purged {count_before:,} forecast records{Style.RESET_ALL}")
    
    return count_before

def generate_forecasts_for_stock(conn):
    """Generate forecasts for all products in stock"""
    print_header("Generating Forecasts for Products in Stock")
    
    # Get products in stock
    products_in_stock = get_products_in_stock(conn)
    print(f"Found {len(products_in_stock):,} products in stock")
    
    # Generate single run ID for this batch
    forecast_date = datetime.now().date()
    run_id = f"RUN_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
    
    print(f"\nForecast Date: {forecast_date}")
    print(f"Run ID: {run_id} (shared for all forecasts in this run)")
    
    # Prepare forecast records
    all_forecasts = []
    products_with_forecast = 0
    products_without_history = 0
    
    print("\nGenerating forecasts...")
    for _, product in tqdm(products_in_stock.iterrows(), total=len(products_in_stock), desc="Processing products"):
        product_id = product['productId']
        
        # Get historical sales
        historical_sales = get_historical_sales(conn, product_id, days=180)
        
        if historical_sales.empty:
            products_without_history += 1
        
        # Calculate forecast
        forecast_df = calculate_forecast(historical_sales, horizon_days=7)
        
        # Create forecast records
        for _, forecast in forecast_df.iterrows():
            target_date = forecast_date + timedelta(days=forecast['day_offset'])
            
            # Generate unique forecast_id for each record
            forecast_id = f"FCT_{uuid.uuid4().hex[:16]}"
            
            forecast_record = {
                'forecast_id': forecast_id,
                'run_id': run_id,
                'forecast_date': forecast_date,
                'productId': product_id,
                'target_date': target_date,
                'forecast_horizon': forecast['day_offset'],
                'predicted_quantity': float(forecast['predicted_quantity']),
                'confidence_lower': float(forecast['confidence_lower']),
                'confidence_upper': float(forecast['confidence_upper']),
                'model_name': 'HistoricalPattern',
                'created_at': datetime.now()
            }
            all_forecasts.append(forecast_record)
        
        products_with_forecast += 1
    
    # Insert all forecasts
    if all_forecasts:
        forecasts_df = pd.DataFrame(all_forecasts)
        
        # Insert using executemany for better performance
        conn.executemany("""
            INSERT INTO forecasts (
                forecast_id, run_id, forecast_date, productId, target_date,
                forecast_horizon, predicted_quantity, confidence_lower, confidence_upper,
                model_name, created_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        """, forecasts_df.values.tolist())
        
        print(f"\n{Fore.GREEN}✓ Generated {len(all_forecasts):,} forecast records{Style.RESET_ALL}")
        print(f"  - Products with forecasts: {products_with_forecast:,}")
        print(f"  - Products without sales history: {products_without_history:,}")
        print(f"  - Forecast horizon: 7 days")
        print(f"  - Historical window: 180 days")
    
    return run_id, len(all_forecasts)

def verify_forecasts(conn, run_id):
    """Verify the generated forecasts"""
    print_header("Verifying Forecasts")
    
    # Check forecast summary
    summary = conn.execute(f"""
        SELECT 
            COUNT(*) as total_records,
            COUNT(DISTINCT productId) as unique_products,
            MIN(target_date) as min_target,
            MAX(target_date) as max_target,
            AVG(predicted_quantity) as avg_predicted,
            MIN(predicted_quantity) as min_predicted,
            MAX(predicted_quantity) as max_predicted
        FROM forecasts
        WHERE run_id = '{run_id}'
    """).fetchone()
    
    print(f"Forecast Summary:")
    print(f"  - Total records: {summary[0]:,}")
    print(f"  - Unique products: {summary[1]:,}")
    print(f"  - Target date range: {summary[2]} to {summary[3]}")
    print(f"  - Avg predicted quantity: {summary[4]:.2f}")
    print(f"  - Min predicted quantity: {summary[5]:.2f}")
    print(f"  - Max predicted quantity: {summary[6]:.2f}")
    
    # Sample forecasts
    print("\nSample forecasts:")
    samples = conn.execute(f"""
        SELECT 
            p.name as product_name,
            f.target_date,
            f.predicted_quantity,
            f.confidence_lower,
            f.confidence_upper
        FROM forecasts f
        JOIN products p ON f.productId = p.productId
        WHERE f.run_id = '{run_id}'
        ORDER BY f.predicted_quantity DESC
        LIMIT 5
    """).fetchall()
    
    for product, target, pred, lower, upper in samples:
        print(f"  - {product[:30]:30} | {target} | Qty: {pred:6.1f} [{lower:6.1f} - {upper:6.1f}]")

def main():
    """Main execution function"""
    print_header("Forecast Purge and Regeneration")
    print(f"Timestamp: {datetime.now()}")
    
    # Connect to database
    conn = duckdb.connect('../data/grocery_final.db')
    
    try:
        # Start transaction
        conn.begin()
        
        # Purge existing forecasts
        purged_count = purge_existing_forecasts(conn)
        
        # Generate new forecasts
        run_id, forecast_count = generate_forecasts_for_stock(conn)
        
        # Verify forecasts
        verify_forecasts(conn, run_id)
        
        # Commit transaction
        conn.commit()
        print(f"\n{Fore.GREEN}✓ Successfully committed all changes{Style.RESET_ALL}")
        
    except Exception as e:
        # Rollback on error
        conn.rollback()
        print(f"\n{Fore.RED}✗ Error occurred, rolling back: {str(e)}{Style.RESET_ALL}")
        raise
    
    finally:
        conn.close()
    
    print_header("Forecast Generation Complete")
    print(f"{Fore.GREEN}✓ Purged {purged_count:,} old forecasts{Style.RESET_ALL}")
    print(f"{Fore.GREEN}✓ Generated {forecast_count:,} new forecasts{Style.RESET_ALL}")
    print(f"{Fore.GREEN}✓ Run ID: {run_id}{Style.RESET_ALL}")

if __name__ == "__main__":
    main()