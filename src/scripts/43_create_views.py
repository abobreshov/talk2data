#!/usr/bin/env python3
"""
Create all analytical views in the final database.
Consolidates view creation from various scripts.
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

def create_all_views():
    """Create all analytical views in the database"""
    
    print_header("Creating Analytical Views")
    print(f"Timestamp: {datetime.now()}")
    
    # Connect to database
    conn = duckdb.connect(str(DB_PATH))
    
    views = [
        {
            "name": "customer_analytics",
            "description": "Customer lifetime value and order statistics",
            "sql": """
                CREATE OR REPLACE VIEW customer_analytics AS
                SELECT 
                    c.customerId,
                    c.first_name,
                    c.last_name,
                    c.email,
                    c.city,
                    c.postcode,
                    COUNT(DISTINCT o.orderId) as total_orders,
                    SUM(CASE WHEN o.orderStatus = 'DELIVERED' THEN 1 ELSE 0 END) as delivered_orders,
                    SUM(CASE WHEN o.orderStatus = 'CANCELLED' THEN 1 ELSE 0 END) as cancelled_orders,
                    SUM(CASE WHEN o.orderStatus = 'DELIVERED' THEN o.totalAmount ELSE 0 END) as lifetime_value,
                    AVG(CASE WHEN o.orderStatus = 'DELIVERED' THEN o.totalAmount END) as avg_order_value,
                    MIN(o.orderDate) as first_order,
                    MAX(o.orderDate) as last_order
                FROM customers c
                LEFT JOIN orders o ON c.customerId = o.customerId
                GROUP BY c.customerId, c.first_name, c.last_name, c.email, c.city, c.postcode
            """
        },
        {
            "name": "product_performance",
            "description": "Product sales performance metrics",
            "sql": """
                CREATE OR REPLACE VIEW product_performance AS
                SELECT 
                    p.productId,
                    p.name,
                    p.brandName,
                    p.category,
                    p.subcategory,
                    p.price_gbp,
                    COUNT(DISTINCT s.orderId) as times_ordered,
                    SUM(s.quantity) as units_sold,
                    SUM(s.unitPrice * s.quantity) as revenue,
                    RANK() OVER (ORDER BY SUM(s.quantity) DESC) as sales_rank
                FROM products p
                LEFT JOIN sales s ON p.productId = s.productId
                GROUP BY p.productId, p.name, p.brandName, p.category, p.subcategory, p.price_gbp
            """
        },
        {
            "name": "stock_availability",
            "description": "Current stock levels by product",
            "sql": """
                CREATE OR REPLACE VIEW stock_availability AS
                SELECT 
                    s.productId,
                    p.name as product_name,
                    p.category,
                    COUNT(DISTINCT s.sku) as num_skus,
                    SUM(s.quantity_in_stock) as total_quantity,
                    MIN(s.expiration_date) as earliest_expiration,
                    SUM(CASE WHEN s.expiration_date <= CURRENT_DATE + INTERVAL '7 days' 
                        THEN s.quantity_in_stock ELSE 0 END) as expiring_week_qty,
                    AVG(s.purchase_price) as avg_purchase_price
                FROM stock s
                INNER JOIN products p ON s.productId = p.productId
                WHERE s.stock_status = 'AVAILABLE'
                GROUP BY s.productId, p.name, p.category
            """
        },
        {
            "name": "latest_forecasts",
            "description": "Most recent forecast for each product",
            "sql": """
                CREATE OR REPLACE VIEW latest_forecasts AS
                SELECT f.*
                FROM forecasts f
                INNER JOIN (
                    SELECT productId, MAX(forecast_date) as max_forecast_date
                    FROM forecasts
                    GROUP BY productId
                ) latest ON f.productId = latest.productId 
                AND f.forecast_date = latest.max_forecast_date
            """
        },
        {
            "name": "pending_deliveries",
            "description": "Upcoming inbound deliveries",
            "sql": """
                CREATE OR REPLACE VIEW pending_deliveries AS
                SELECT 
                    id.productId,
                    p.name as product_name,
                    id.expected_delivery_date,
                    SUM(id.quantity) as total_quantity,
                    COUNT(DISTINCT id.po_id) as num_orders,
                    AVG(id.unit_cost) as avg_unit_cost
                FROM inbound_deliveries id
                INNER JOIN products p ON id.productId = p.productId
                WHERE id.status = 'PENDING'
                AND id.expected_delivery_date >= CURRENT_DATE
                GROUP BY id.productId, p.name, id.expected_delivery_date
                ORDER BY id.expected_delivery_date, id.productId
            """
        },
        {
            "name": "product_catalog",
            "description": "Product information with SKU details",
            "sql": """
                CREATE OR REPLACE VIEW product_catalog AS
                SELECT 
                    p.productId,
                    p.name,
                    p.brandName,
                    p.sellingSize,
                    p.category,
                    p.subcategory,
                    p.price_gbp,
                    ps.sku as primary_sku,
                    COUNT(DISTINCT ps2.sku) as total_skus
                FROM products p
                LEFT JOIN product_skus ps ON p.productId = ps.productId 
                    AND ps.is_primary = TRUE
                LEFT JOIN product_skus ps2 ON p.productId = ps2.productId
                GROUP BY p.productId, p.name, p.brandName, p.sellingSize, 
                         p.category, p.subcategory, p.price_gbp, ps.sku
            """
        }
    ]
    
    # Create each view
    created = 0
    for view in views:
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
    print(f"Created {created} out of {len(views)} views")
    
    # List all views
    all_views = conn.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_type = 'VIEW'
        ORDER BY table_name
    """).fetchall()
    
    print(f"\nTotal views in database: {len(all_views)}")
    for view in all_views:
        print(f"  - {view[0]}")
    
    conn.close()
    print(f"\n✓ View creation completed")

if __name__ == "__main__":
    create_all_views()