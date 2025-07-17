#!/usr/bin/env python3
"""
Generate live basket (ITB - In The Basket) data
- Day 1 (last sales + 1): Random deviation from forecast
- Day 2: 15% less fulfillment 
- Days 3-7: Progressive reduction (15-45% unfulfilled)
"""

import duckdb
from pathlib import Path
from datetime import datetime, timedelta
import random
import numpy as np
import pandas as pd
import uuid

DATA_DIR = Path(__file__).parent.parent / 'data'
DB_PATH = DATA_DIR / 'grocery_final.db'

def generate_live_basket():
    """Generate realistic live basket data based on forecasts"""
    
    print("=== Generating Live Basket (ITB) Data ===\n")
    
    try:
        conn = duckdb.connect(str(DB_PATH))
        
        # 1. Get last sales date and forecast data
        print("1. Loading forecast and sales data...")
        
        last_sale_date = conn.execute("SELECT MAX(CAST(saleDate AS DATE)) FROM sales").fetchone()[0]
        print(f"Last sales date: {last_sale_date}")
        
        # Start from last sale + 1 day
        start_date = last_sale_date + timedelta(days=1)
        
        # Get forecasts for next 7 days
        forecasts = conn.execute("""
            SELECT 
                f.productId,
                f.target_date,
                f.predicted_quantity as forecast_qty,
                p.name as product_name,
                p.price_gbp
            FROM latest_forecasts f
            JOIN products p ON f.productId = p.productId
            WHERE f.target_date >= ?
            AND f.target_date <= ?
            ORDER BY f.target_date, f.productId
        """, [start_date, start_date + timedelta(days=6)]).fetchdf()
        
        print(f"Found {len(forecasts)} forecast records for next 7 days")
        
        # 2. Get customer base for basket generation
        print("\n2. Loading customer data...")
        customers = conn.execute("""
            SELECT 
                customerId,
                COUNT(DISTINCT orderId) as order_count
            FROM orders
            WHERE orderStatus IN ('DELIVERED', 'PICKED')
            GROUP BY customerId
            HAVING order_count > 10  -- Active customers
            ORDER BY RANDOM()
            LIMIT 500  -- Use subset of active customers
        """).fetchdf()
        
        customer_ids = customers['customerId'].tolist()
        print(f"Using {len(customer_ids)} active customers")
        
        # 3. Generate basket entries
        print("\n3. Generating basket entries...")
        
        basket_entries = []
        basket_id = 1
        
        # Define fulfillment rates by day
        fulfillment_rates = {
            0: 1.0,    # Day 1: 100% (with random variation)
            1: 0.85,   # Day 2: 85% (15% unfulfilled)
            2: 0.75,   # Day 3: 75%
            3: 0.65,   # Day 4: 65%
            4: 0.60,   # Day 5: 60%
            5: 0.55,   # Day 6: 55%
            6: 0.55    # Day 7: 55%
        }
        
        # Group forecasts by date
        for target_date, date_forecasts in forecasts.groupby('target_date'):
            # Convert to date if needed
            if hasattr(target_date, 'date'):
                target_date_obj = target_date.date()
            else:
                target_date_obj = target_date
            days_ahead = (target_date_obj - start_date).days
            fulfillment_rate = fulfillment_rates.get(days_ahead, 0.55)
            
            print(f"\n  Processing {target_date} (Day {days_ahead + 1}):")
            print(f"  - Fulfillment rate: {fulfillment_rate * 100:.0f}%")
            
            # Sample products for this day (not all forecasted products will have baskets)
            num_products = int(len(date_forecasts) * fulfillment_rate * random.uniform(0.8, 1.0))
            sampled_products = date_forecasts.sample(n=min(num_products, len(date_forecasts)))
            
            daily_baskets = 0
            
            for _, forecast in sampled_products.iterrows():
                # Determine how many customers have this product in basket
                # Popular products have more customers
                popularity_factor = min(forecast['forecast_qty'] / 100, 1.0)
                num_customers = max(1, int(len(customer_ids) * popularity_factor * 0.1))
                num_customers = min(num_customers, int(forecast['forecast_qty'] * 0.3))
                
                # Sample customers for this product
                selected_customers = random.sample(customer_ids, min(num_customers, len(customer_ids)))
                
                # Distribute forecast quantity among customers
                remaining_qty = forecast['forecast_qty'] * fulfillment_rate
                
                for customer_id in selected_customers:
                    if remaining_qty <= 0:
                        break
                    
                    # Generate realistic quantity per customer
                    if forecast['price_gbp'] > 10:  # Expensive items
                        qty = 1
                    elif forecast['price_gbp'] > 5:  # Medium price
                        qty = random.randint(1, 2)
                    else:  # Cheap items
                        qty = random.randint(1, 5)
                    
                    # Don't exceed remaining forecast
                    qty = min(qty, int(remaining_qty))
                    
                    if qty > 0:
                        # Create session ID (customers can have multiple sessions)
                        session_id = f"SES_{customer_id}_{target_date.strftime('%Y%m%d')}_{uuid.uuid4().hex[:8]}"
                        
                        # Calculate basket expiration (2-4 hours from now)
                        # Use target_date_obj which is already converted to date
                        added_at = datetime.now() - timedelta(days=(last_sale_date - target_date_obj).days)
                        expires_at = added_at + timedelta(hours=random.randint(2, 4))
                        
                        basket_entries.append({
                            'basket_id': basket_id,
                            'session_id': session_id,
                            'customerId': customer_id,
                            'productId': forecast['productId'],
                            'quantity': qty,
                            'added_at': added_at,
                            'expires_at': expires_at,
                            'status': 'ACTIVE'
                        })
                        
                        basket_id += 1
                        daily_baskets += 1
                        remaining_qty -= qty
            
            print(f"  - Generated {daily_baskets} basket entries")
        
        # 4. Insert into database
        print(f"\n4. Inserting {len(basket_entries)} basket entries...")
        
        if basket_entries:
            # Clear existing data
            conn.execute("DELETE FROM live_basket")
            
            # Insert new data
            basket_df = pd.DataFrame(basket_entries)
            conn.execute("INSERT INTO live_basket SELECT * FROM basket_df")
            
            print(f"✓ Inserted {len(basket_entries)} basket entries")
        
        # 5. Analyze ITB vs Forecast alignment
        print("\n5. ITB vs Forecast Analysis:")
        
        itb_analysis = conn.execute("""
            WITH itb_daily AS (
                SELECT 
                    CAST(added_at AS DATE) as basket_date,
                    productId,
                    SUM(quantity) as itb_quantity
                FROM live_basket
                WHERE status = 'ACTIVE'
                GROUP BY CAST(added_at AS DATE), productId
            ),
            forecast_daily AS (
                SELECT 
                    target_date,
                    productId,
                    predicted_quantity as forecast_quantity
                FROM latest_forecasts
                WHERE target_date >= ? 
                AND target_date <= ?
            )
            SELECT 
                f.target_date,
                COUNT(DISTINCT f.productId) as products_forecasted,
                COUNT(DISTINCT i.productId) as products_in_basket,
                SUM(f.forecast_quantity) as total_forecast,
                COALESCE(SUM(i.itb_quantity), 0) as total_itb,
                ROUND(COALESCE(SUM(i.itb_quantity), 0) * 100.0 / SUM(f.forecast_quantity), 1) as itb_percentage
            FROM forecast_daily f
            LEFT JOIN itb_daily i ON f.productId = i.productId 
                AND f.target_date = i.basket_date
            GROUP BY f.target_date
            ORDER BY f.target_date
        """, [start_date, start_date + timedelta(days=6)]).fetchdf()
        
        print("\nDaily ITB Fulfillment:")
        print(itb_analysis)
        
        # 6. Check inbound coverage
        print("\n6. Checking Inbound Coverage vs Demand:")
        
        coverage_analysis = conn.execute("""
            WITH daily_demand AS (
                -- Forecast + ITB = Total Demand
                SELECT 
                    f.target_date as demand_date,
                    f.productId,
                    f.predicted_quantity as forecast_qty,
                    COALESCE(itb.itb_qty, 0) as itb_qty,
                    f.predicted_quantity + COALESCE(itb.itb_qty, 0) as total_demand
                FROM latest_forecasts f
                LEFT JOIN (
                    SELECT 
                        CAST(added_at AS DATE) as basket_date,
                        productId,
                        SUM(quantity) as itb_qty
                    FROM live_basket
                    WHERE status = 'ACTIVE'
                    GROUP BY CAST(added_at AS DATE), productId
                ) itb ON f.productId = itb.productId 
                    AND f.target_date = itb.basket_date
                WHERE f.target_date >= ?
                AND f.target_date <= ?
            ),
            daily_inbound AS (
                SELECT 
                    expected_delivery_date,
                    productId,
                    SUM(quantity) as inbound_qty
                FROM inbound_deliveries
                WHERE status = 'PENDING'
                AND expected_delivery_date >= ?
                AND expected_delivery_date <= ?
                GROUP BY expected_delivery_date, productId
            ),
            current_stock AS (
                SELECT 
                    productId,
                    SUM(quantity_in_stock) as stock_qty
                FROM stock
                WHERE stock_status = 'AVAILABLE'
                GROUP BY productId
            )
            SELECT 
                d.demand_date,
                COUNT(DISTINCT d.productId) as products,
                SUM(d.total_demand) as total_demand,
                SUM(COALESCE(i.inbound_qty, 0)) as inbound_supply,
                SUM(COALESCE(s.stock_qty, 0)) as current_stock,
                ROUND((SUM(COALESCE(i.inbound_qty, 0)) + SUM(COALESCE(s.stock_qty, 0))) * 100.0 / 
                      NULLIF(SUM(d.total_demand), 0), 1) as coverage_percentage
            FROM daily_demand d
            LEFT JOIN daily_inbound i ON d.productId = i.productId 
                AND d.demand_date = i.expected_delivery_date
            LEFT JOIN current_stock s ON d.productId = s.productId
            GROUP BY d.demand_date
            ORDER BY d.demand_date
        """, [start_date, start_date + timedelta(days=6)] * 2).fetchdf()
        
        print("\nDemand Coverage Analysis:")
        print(coverage_analysis)
        
        # 7. Sample basket contents
        print("\n7. Sample Basket Contents:")
        
        sample_baskets = conn.execute("""
            SELECT 
                lb.session_id,
                c.first_name || ' ' || c.last_name as customer_name,
                p.name as product_name,
                lb.quantity,
                lb.added_at,
                lb.expires_at
            FROM live_basket lb
            JOIN customers c ON lb.customerId = c.customerId
            JOIN products p ON lb.productId = p.productId
            WHERE lb.status = 'ACTIVE'
            ORDER BY lb.added_at DESC
            LIMIT 10
        """).fetchdf()
        
        print(sample_baskets)
        
        # 8. ITB Summary
        print("\n8. ITB Summary by Category:")
        
        category_summary = conn.execute("""
            SELECT 
                p.category,
                COUNT(DISTINCT lb.productId) as products,
                COUNT(DISTINCT lb.session_id) as sessions,
                SUM(lb.quantity) as total_quantity,
                SUM(lb.quantity * p.price_gbp) as basket_value
            FROM live_basket lb
            JOIN products p ON lb.productId = p.productId
            WHERE lb.status = 'ACTIVE'
            GROUP BY p.category
            ORDER BY basket_value DESC
            LIMIT 10
        """).fetchdf()
        
        print(category_summary)
        
        conn.close()
        print("\n✓ Live basket generation completed successfully!")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        raise

if __name__ == "__main__":
    generate_live_basket()