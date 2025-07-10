#!/usr/bin/env python3
"""
Create final database with proper structure and naming conventions
- Products table with productId (not SKU in the column name)
- Product_SKUs mapping table (one product -> multiple warehouse SKUs)
- All other tables with proper foreign key relationships
"""

import os
import sys
import pandas as pd
import duckdb
from pathlib import Path
from datetime import datetime
import json

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

# Load configuration
config_path = Path(__file__).parent / "config.json"
with open(config_path, 'r') as f:
    config = json.load(f)

DATA_DIR = Path(config['data_dir'])
PRODUCTS_DUCKDB = DATA_DIR / 'products.duckdb'
ORDERS_DB = Path(config['orders_db'])
FINAL_DB = DATA_DIR / 'grocery_final.db'

def create_final_database():
    """Create final database with proper structure"""
    print("=" * 60)
    print("Creating Final Database with Proper Structure")
    print("=" * 60)
    print(f"Execution time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Remove existing database if it exists
    if FINAL_DB.exists():
        FINAL_DB.unlink()
        print(f"\n✓ Removed existing {FINAL_DB.name}")
    
    # Create new database
    print(f"\n=== Creating New Database: {FINAL_DB.name} ===")
    conn = duckdb.connect(str(FINAL_DB))
    
    try:
        # 1. Create schema with proper naming
        print("\n=== Creating Schema ===")
        
        # Products table - using productId as primary key
        conn.execute("""
            CREATE TABLE products (
                productId VARCHAR PRIMARY KEY,
                name VARCHAR NOT NULL,
                brandName VARCHAR,
                sellingSize VARCHAR,
                currency VARCHAR DEFAULT 'GBP',
                price_pence INTEGER NOT NULL CHECK (price_pence > 0),
                price_gbp DECIMAL(10,2) GENERATED ALWAYS AS (price_pence / 100.0) VIRTUAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        print("✓ Created products table")
        
        # Product SKUs mapping table
        conn.execute("""
            CREATE TABLE product_skus (
                productId VARCHAR NOT NULL,
                sku VARCHAR NOT NULL,
                sku_suffix VARCHAR,
                is_primary BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (productId, sku),
                FOREIGN KEY (productId) REFERENCES products(productId)
            )
        """)
        print("✓ Created product_skus mapping table")
        
        # Customers table (updated structure: removed ip_address, added city)
        conn.execute("""
            CREATE TABLE customers (
                customerId INTEGER PRIMARY KEY,
                first_name VARCHAR NOT NULL,
                last_name VARCHAR NOT NULL,
                email VARCHAR UNIQUE NOT NULL,
                gender VARCHAR,
                address VARCHAR,
                city VARCHAR,
                postcode VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        print("✓ Created customers table")
        
        # Orders table
        conn.execute("""
            CREATE TABLE orders (
                orderId VARCHAR PRIMARY KEY,
                customerId INTEGER NOT NULL,
                orderDate TIMESTAMP NOT NULL,
                deliveryDate TIMESTAMP NOT NULL,
                orderStatus VARCHAR NOT NULL CHECK (orderStatus IN ('DELIVERED', 'CANCELLED', 'PICKED', 'FUTURE')),
                totalAmount DECIMAL(10,2) NOT NULL CHECK (totalAmount >= 0),
                itemCount INTEGER NOT NULL CHECK (itemCount >= 0),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (customerId) REFERENCES customers(customerId)
            )
        """)
        print("✓ Created orders table")
        
        # Order items table - references productId
        conn.execute("""
            CREATE TABLE order_items (
                orderItemId VARCHAR PRIMARY KEY,
                orderId VARCHAR NOT NULL,
                productId VARCHAR NOT NULL,
                quantity INTEGER NOT NULL CHECK (quantity > 0),
                unitPrice DECIMAL(10,2) NOT NULL CHECK (unitPrice > 0),
                totalPrice DECIMAL(10,2) NOT NULL CHECK (totalPrice > 0),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (orderId) REFERENCES orders(orderId),
                FOREIGN KEY (productId) REFERENCES products(productId)
            )
        """)
        print("✓ Created order_items table")
        
        # Sales table - references productId
        conn.execute("""
            CREATE TABLE sales (
                saleId VARCHAR PRIMARY KEY,
                orderId VARCHAR NOT NULL,
                orderItemId VARCHAR NOT NULL,
                customerId INTEGER NOT NULL,
                productId VARCHAR NOT NULL,
                saleDate TIMESTAMP NOT NULL,
                unitPrice DECIMAL(10,2) NOT NULL CHECK (unitPrice > 0),
                quantity INTEGER NOT NULL CHECK (quantity > 0),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (orderId) REFERENCES orders(orderId),
                FOREIGN KEY (orderItemId) REFERENCES order_items(orderItemId),
                FOREIGN KEY (customerId) REFERENCES customers(customerId),
                FOREIGN KEY (productId) REFERENCES products(productId)
            )
        """)
        print("✓ Created sales table")
        
        # 2. Load data from existing sources
        print("\n=== Loading Data ===")
        
        # Attach source databases
        conn.execute(f"ATTACH '{PRODUCTS_DUCKDB}' AS products_source")
        conn.execute(f"ATTACH '{ORDERS_DB}' AS orders_source")
        
        # Load products (SKU column is actually productId)
        print("\nLoading products...")
        conn.execute("""
            INSERT INTO products (productId, name, brandName, sellingSize, currency, price_pence)
            SELECT 
                sku as productId,  -- SKU column contains productId
                name,
                brandName,
                sellingSize,
                currency,
                price
            FROM products_source.products
        """)
        product_count = conn.execute("SELECT COUNT(*) FROM products").fetchone()[0]
        print(f"✓ Loaded {product_count:,} products")
        
        # Load product SKUs mapping
        print("\nLoading product SKUs mapping...")
        conn.execute("""
            INSERT INTO product_skus (productId, sku, sku_suffix, is_primary)
            SELECT 
                productId,
                sku,
                sku_suffix,
                CASE WHEN sku_suffix = '0001' THEN TRUE ELSE FALSE END
            FROM products_source.product_skus
        """)
        sku_count = conn.execute("SELECT COUNT(*) FROM product_skus").fetchone()[0]
        print(f"✓ Loaded {sku_count:,} SKU mappings")
        
        # Load customers from CSV (rename id to customerId)
        print("\nLoading customers...")
        customers_csv = DATA_DIR / 'customers.csv'
        customers_df = pd.read_csv(customers_csv)
        if 'id' in customers_df.columns:
            customers_df = customers_df.rename(columns={'id': 'customerId'})
        
        conn.execute("""
            INSERT INTO customers (customerId, first_name, last_name, email, gender, address, city, postcode)
            SELECT customerId, first_name, last_name, email, gender, address, city, postcode
            FROM customers_df
        """)
        customer_count = conn.execute("SELECT COUNT(*) FROM customers").fetchone()[0]
        print(f"✓ Loaded {customer_count:,} customers")
        
        # Load orders
        print("\nLoading orders...")
        conn.execute("""
            INSERT INTO orders (orderId, customerId, orderDate, deliveryDate, orderStatus, totalAmount, itemCount)
            SELECT orderId, customerId, orderDate, deliveryDate, orderStatus, totalAmount, itemCount
            FROM orders_source.orders
        """)
        order_count = conn.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
        print(f"✓ Loaded {order_count:,} orders")
        
        # Load order items (need to handle productId format)
        print("\nLoading order items...")
        conn.execute("""
            INSERT INTO order_items (orderItemId, orderId, productId, quantity, unitPrice, totalPrice)
            SELECT 
                orderItemId, 
                orderId,
                -- Ensure productId matches our products table format
                LPAD(CAST(productId AS VARCHAR), 18, '0') as productId,
                quantity, 
                unitPrice, 
                totalPrice
            FROM orders_source.order_items
        """)
        item_count = conn.execute("SELECT COUNT(*) FROM order_items").fetchone()[0]
        print(f"✓ Loaded {item_count:,} order items")
        
        # Load sales (need to handle productId format)
        print("\nLoading sales...")
        conn.execute("""
            INSERT INTO sales (saleId, orderId, orderItemId, customerId, productId, saleDate, unitPrice, quantity)
            SELECT 
                saleId, 
                orderId, 
                orderItemId, 
                customerId,
                -- Ensure productId matches our products table format
                LPAD(CAST(productId AS VARCHAR), 18, '0') as productId,
                saleDate, 
                unitPrice, 
                quantity
            FROM orders_source.sales
        """)
        sales_count = conn.execute("SELECT COUNT(*) FROM sales").fetchone()[0]
        print(f"✓ Loaded {sales_count:,} sales records")
        
        # 3. Create indexes
        print("\n=== Creating Indexes ===")
        
        # Products indexes
        conn.execute("CREATE INDEX idx_products_name ON products(name)")
        conn.execute("CREATE INDEX idx_products_brand ON products(brandName)")
        conn.execute("CREATE INDEX idx_products_price ON products(price_pence)")
        
        # Product SKUs indexes
        conn.execute("CREATE INDEX idx_skus_sku ON product_skus(sku)")
        conn.execute("CREATE INDEX idx_skus_primary ON product_skus(is_primary)")
        
        # Other indexes
        conn.execute("CREATE INDEX idx_customers_email ON customers(email)")
        conn.execute("CREATE INDEX idx_customers_postcode ON customers(postcode)")
        conn.execute("CREATE INDEX idx_orders_customer ON orders(customerId)")
        conn.execute("CREATE INDEX idx_orders_date ON orders(orderDate)")
        conn.execute("CREATE INDEX idx_orders_status ON orders(orderStatus)")
        conn.execute("CREATE INDEX idx_items_order ON order_items(orderId)")
        conn.execute("CREATE INDEX idx_items_product ON order_items(productId)")
        conn.execute("CREATE INDEX idx_sales_date ON sales(saleDate)")
        conn.execute("CREATE INDEX idx_sales_product ON sales(productId)")
        
        print("✓ Created all indexes")
        
        # 4. Create analytical views
        print("\n=== Creating Views ===")
        
        # Product catalog view (with primary SKU)
        conn.execute("""
            CREATE VIEW product_catalog AS
            SELECT 
                p.productId,
                p.name,
                p.brandName,
                p.sellingSize,
                p.price_gbp,
                ps.sku as primary_sku,
                COUNT(DISTINCT ps2.sku) as total_skus
            FROM products p
            LEFT JOIN product_skus ps ON p.productId = ps.productId AND ps.is_primary = TRUE
            LEFT JOIN product_skus ps2 ON p.productId = ps2.productId
            GROUP BY p.productId, p.name, p.brandName, p.sellingSize, p.price_gbp, ps.sku
        """)
        print("✓ Created product_catalog view")
        
        # Customer analytics view (updated to include city)
        conn.execute("""
            CREATE VIEW customer_analytics AS
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
                AVG(CASE WHEN o.orderStatus = 'DELIVERED' THEN o.totalAmount ELSE NULL END) as avg_order_value,
                MIN(o.orderDate) as first_order,
                MAX(o.orderDate) as last_order
            FROM customers c
            LEFT JOIN orders o ON c.customerId = o.customerId
            GROUP BY c.customerId, c.first_name, c.last_name, c.email, c.city, c.postcode
        """)
        print("✓ Created customer_analytics view")
        
        # Product performance view
        conn.execute("""
            CREATE VIEW product_performance AS
            SELECT 
                p.productId,
                p.name,
                p.brandName,
                p.price_gbp,
                COUNT(DISTINCT s.orderId) as times_ordered,
                SUM(s.quantity) as units_sold,
                SUM(s.unitPrice * s.quantity) as revenue,
                RANK() OVER (ORDER BY SUM(s.quantity) DESC) as sales_rank
            FROM products p
            LEFT JOIN sales s ON p.productId = s.productId
            GROUP BY p.productId, p.name, p.brandName, p.price_gbp
        """)
        print("✓ Created product_performance view")
        
        # 5. Verify data integrity
        print("\n=== Verifying Data Integrity ===")
        
        # Check product-SKU relationships
        products_with_skus = conn.execute("""
            SELECT COUNT(DISTINCT productId) 
            FROM product_skus
        """).fetchone()[0]
        
        print(f"✓ {products_with_skus} products have SKU mappings")
        
        # Check average SKUs per product
        avg_skus = conn.execute("""
            SELECT AVG(sku_count) 
            FROM (
                SELECT productId, COUNT(*) as sku_count 
                FROM product_skus 
                GROUP BY productId
            )
        """).fetchone()[0]
        
        print(f"✓ Average SKUs per product: {avg_skus:.1f}")
        
        # 6. Display summary
        print("\n=== Database Summary ===")
        
        tables = ['products', 'product_skus', 'customers', 'orders', 'order_items', 'sales']
        for table in tables:
            count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            print(f"  {table}: {count:,} records")
        
        # Sample product with multiple SKUs
        print("\n=== Sample Product with Multiple SKUs ===")
        sample = conn.execute("""
            SELECT 
                p.productId,
                p.name,
                p.brandName,
                p.price_gbp,
                ps.sku,
                ps.sku_suffix,
                ps.is_primary
            FROM products p
            JOIN product_skus ps ON p.productId = ps.productId
            WHERE p.productId IN (
                SELECT productId 
                FROM product_skus 
                GROUP BY productId 
                HAVING COUNT(*) > 1
                LIMIT 1
            )
            ORDER BY ps.sku
        """).fetchall()
        
        if sample:
            print(f"\nProduct: {sample[0][1]} ({sample[0][2]})")
            print(f"ProductId: {sample[0][0]}")
            print(f"Price: £{sample[0][3]:.2f}")
            print("SKUs:")
            for row in sample:
                primary = " (PRIMARY)" if row[6] else ""
                print(f"  - {row[4]} (suffix: {row[5]}){primary}")
        
        # Save database info
        db_info = {
            'database_path': str(FINAL_DB),
            'creation_timestamp': datetime.now().isoformat(),
            'structure': {
                'products': {
                    'count': product_count,
                    'primary_key': 'productId',
                    'description': 'Product catalog with productId as primary key'
                },
                'product_skus': {
                    'count': sku_count,
                    'primary_key': '(productId, sku)',
                    'description': 'Maps products to multiple warehouse SKUs'
                },
                'customers': customer_count,
                'orders': order_count,
                'order_items': item_count,
                'sales': sales_count
            },
            'relationships': [
                'product_skus.productId -> products.productId',
                'orders.customerId -> customers.customerId',
                'order_items.orderId -> orders.orderId',
                'order_items.productId -> products.productId',
                'sales references all tables'
            ]
        }
        
        with open(DATA_DIR / 'final_database_info.json', 'w') as f:
            json.dump(db_info, f, indent=2)
        
        print("\n" + "=" * 60)
        print("✓ FINAL DATABASE CREATED SUCCESSFULLY")
        print(f"\nDatabase location: {FINAL_DB}")
        print("Database info saved to: final_database_info.json")
        
    except Exception as e:
        print(f"\n✗ Error creating database: {e}")
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    create_final_database()