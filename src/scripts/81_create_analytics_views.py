#!/usr/bin/env python3
"""
Create additional analytical views for reporting and analysis.
Extends the basic views with more complex analytics.
"""

import duckdb
from pathlib import Path
from datetime import datetime
from colorama import init, Fore, Style

# Initialize colorama
init()

DATA_DIR = Path(__file__).parent.parent / 'data'
DB_PATH = DATA_DIR / 'grocery_final.db'

def print_header(text):
    """Print formatted header"""
    print(f"\n{Fore.CYAN}{'='*60}{Style.RESET_ALL}")
    print(f"{Fore.CYAN}{text}{Style.RESET_ALL}")
    print(f"{Fore.CYAN}{'='*60}{Style.RESET_ALL}")

def create_analytics_views():
    """Create advanced analytical views"""
    
    print_header("Creating Advanced Analytics Views")
    print(f"Timestamp: {datetime.now()}")
    
    # Connect to database
    conn = duckdb.connect(str(DB_PATH))
    
    analytics_views = [
        {
            "name": "stock_summary",
            "description": "Comprehensive stock summary by product",
            "sql": """
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
                INNER JOIN products p ON s.productId = p.productId
                WHERE s.stock_status = 'AVAILABLE'
                GROUP BY s.productId, p.name, p.category, p.subcategory
            """
        },
        {
            "name": "supplier_delivery_calendar",
            "description": "Supplier delivery schedule with day names",
            "sql": """
                CREATE OR REPLACE VIEW supplier_delivery_calendar AS
                SELECT 
                    s.supplier_id,
                    s.supplier_name,
                    s.contact_email,
                    ss.delivery_date,
                    ss.po_cutoff_date,
                    ss.po_cutoff_time,
                    ss.lead_time_days,
                    CASE 
                        WHEN DATE_PART('DOW', ss.delivery_date) = 0 THEN 'Sunday'
                        WHEN DATE_PART('DOW', ss.delivery_date) = 1 THEN 'Monday'
                        WHEN DATE_PART('DOW', ss.delivery_date) = 2 THEN 'Tuesday'
                        WHEN DATE_PART('DOW', ss.delivery_date) = 3 THEN 'Wednesday'
                        WHEN DATE_PART('DOW', ss.delivery_date) = 4 THEN 'Thursday'
                        WHEN DATE_PART('DOW', ss.delivery_date) = 5 THEN 'Friday'
                        WHEN DATE_PART('DOW', ss.delivery_date) = 6 THEN 'Saturday'
                    END as delivery_day_of_week
                FROM supplier_schedules ss
                INNER JOIN suppliers s ON ss.supplier_id = s.supplier_id
            """
        },
        {
            "name": "future_purges",
            "description": "Products that will expire in the future",
            "sql": """
                CREATE OR REPLACE VIEW future_purges AS
                SELECT 
                    s.sku,
                    s.productId,
                    p.name as product_name,
                    s.quantity_in_stock as quantity_to_purge,
                    s.expiration_date as purge_date,
                    s.batch_number,
                    CASE 
                        WHEN s.expiration_date < CURRENT_DATE THEN 'EXPIRED'
                        WHEN s.expiration_date = CURRENT_DATE THEN 'EXPIRING_TODAY'
                        WHEN s.expiration_date <= CURRENT_DATE + INTERVAL '3 days' THEN 'EXPIRING_SOON'
                        ELSE 'FUTURE'
                    END as purge_status
                FROM stock s
                INNER JOIN products p ON s.productId = p.productId
                WHERE s.quantity_in_stock > 0
                AND s.stock_status = 'AVAILABLE'
                ORDER BY s.expiration_date, s.productId
            """
        },
        {
            "name": "stock_by_zone",
            "description": "Stock levels by temperature zone",
            "sql": """
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
            """
        },
        {
            "name": "product_expiration_guide",
            "description": "Product shelf life reference guide",
            "sql": """
                CREATE OR REPLACE VIEW product_expiration_guide AS
                SELECT 
                    p.productId,
                    p.name as product_name,
                    p.category,
                    p.subcategory,
                    ppr.shelf_life_days,
                    ppr.min_shelf_life_days,
                    ppr.max_shelf_life_days,
                    ppr.temperature_sensitive,
                    ppr.requires_refrigeration,
                    ppr.notes as storage_notes,
                    p.price_pence,
                    p.price_gbp
                FROM products p
                LEFT JOIN product_purge_reference ppr 
                    ON p.category = ppr.category 
                    AND (p.subcategory = ppr.subcategory 
                         OR (p.subcategory IS NULL AND ppr.subcategory IS NULL))
                ORDER BY ppr.shelf_life_days, p.category, p.subcategory
            """
        },
        {
            "name": "itb_summary",
            "description": "In-The-Basket summary by product",
            "sql": """
                CREATE OR REPLACE VIEW itb_summary AS
                SELECT 
                    lb.productId,
                    p.name as product_name,
                    COUNT(DISTINCT lb.session_id) as active_sessions,
                    SUM(lb.quantity) as total_quantity,
                    AVG(lb.quantity) as avg_quantity_per_session,
                    MIN(lb.added_at) as earliest_add,
                    MAX(lb.added_at) as latest_add
                FROM live_basket lb
                INNER JOIN products p ON lb.productId = p.productId
                WHERE lb.status = 'ACTIVE'
                AND (lb.expires_at IS NULL OR lb.expires_at > CURRENT_TIMESTAMP)
                GROUP BY lb.productId, p.name
            """
        },
        {
            "name": "forecast_accuracy",
            "description": "Compare forecasts to actual sales",
            "sql": """
                CREATE OR REPLACE VIEW forecast_accuracy AS
                SELECT 
                    f.productId,
                    f.forecast_date,
                    f.target_date,
                    f.forecast_horizon,
                    f.predicted_quantity,
                    s.actual_quantity,
                    ABS(f.predicted_quantity - s.actual_quantity) as absolute_error,
                    CASE 
                        WHEN s.actual_quantity > 0 
                        THEN (f.predicted_quantity - s.actual_quantity) / s.actual_quantity
                        ELSE NULL 
                    END as relative_error
                FROM forecasts f
                LEFT JOIN (
                    SELECT 
                        productId,
                        DATE_TRUNC('day', saleDate) as sale_date,
                        SUM(quantity) as actual_quantity
                    FROM sales
                    GROUP BY productId, DATE_TRUNC('day', saleDate)
                ) s ON f.productId = s.productId AND f.target_date = s.sale_date
                WHERE s.actual_quantity IS NOT NULL
            """
        }
    ]
    
    # Create each view
    created = 0
    for view in analytics_views:
        try:
            print(f"\nCreating view: {view['name']}")
            print(f"  Description: {view['description']}")
            
            conn.execute(view['sql'])
            
            # Get row count
            row_count = conn.execute(f"SELECT COUNT(*) FROM {view['name']}").fetchone()[0]
            print(f"  {Fore.GREEN}✓{Style.RESET_ALL} Created successfully ({row_count:,} rows)")
            created += 1
            
        except Exception as e:
            print(f"  {Fore.RED}✗{Style.RESET_ALL} Error: {e}")
    
    # Summary
    print_header("Summary")
    print(f"Created {created} out of {len(analytics_views)} analytics views")
    
    # List all views
    all_views = conn.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_type = 'VIEW'
        ORDER BY table_name
    """).fetchall()
    
    print(f"\nTotal views in database: {len(all_views)}")
    
    # Group views by type
    basic_views = ['customer_analytics', 'product_performance', 'stock_availability', 
                   'latest_forecasts', 'pending_deliveries', 'product_catalog']
    
    print("\nBasic Views:")
    for view in all_views:
        if view[0] in basic_views:
            print(f"  - {view[0]}")
    
    print("\nAnalytics Views:")
    for view in all_views:
        if view[0] not in basic_views:
            print(f"  - {view[0]}")
    
    conn.close()
    print(f"\n✓ Analytics view creation completed")

if __name__ == "__main__":
    create_analytics_views()