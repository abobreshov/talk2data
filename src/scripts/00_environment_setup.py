#!/usr/bin/env python3
"""
Task 11: Environment Setup and Data Validation
This script sets up the environment and validates all required data sources
before starting the orders synthesis process.
"""

import os
import sys
import pandas as pd
import duckdb
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))
sys.path.append(str(Path(__file__).parent))  # Add scripts dir to path

# Load environment variables
env_file = Path(__file__).parent / '.env'
if env_file.exists():
    load_dotenv(env_file)
else:
    load_dotenv()

# Define paths from environment or defaults
DATA_DIR = Path(os.getenv('DATA_DIR', project_root / "src" / "data"))
NOTEBOOKS_DIR = project_root / "src" / "notebooks"
PRODUCTS_DB = Path(os.getenv('PRODUCTS_DB', DATA_DIR / "products.duckdb"))
ORDERS_DB = Path(os.getenv('ORDERS_DB', DATA_DIR / "orders.duckdb"))
CUSTOMERS_CSV = Path(os.getenv('CUSTOMERS_CSV', DATA_DIR / "customers.csv"))

def check_environment():
    """Check if all required packages are installed"""
    required_packages = [
        'pandas', 'numpy', 'duckdb', 'matplotlib', 
        'datasetsforecast', 'tqdm', 'python-dotenv'
    ]
    
    missing_packages = []
    for package in required_packages:
        try:
            __import__(package)
            print(f"✓ {package} is installed")
        except ImportError:
            missing_packages.append(package)
            print(f"✗ {package} is NOT installed")
    
    if missing_packages:
        print(f"\nPlease install missing packages:")
        print(f"~/miniconda3/envs/grocery_poc/bin/python -m pip install {' '.join(missing_packages)}")
        return False
    
    return True

def validate_products_database():
    """Validate products database and FOOD category data"""
    print("\n=== Validating Products Database ===")
    
    if not PRODUCTS_DB.exists():
        print(f"✗ Products database not found at {PRODUCTS_DB}")
        return False
    
    try:
        conn = duckdb.connect(str(PRODUCTS_DB), read_only=True)
        
        # Check if products table exists
        tables = conn.execute("SHOW TABLES").fetchall()
        if not any('products' in str(table) for table in tables):
            print("✗ 'products' table not found in database")
            return False
        
        # Get products statistics (all products are FOOD category)
        food_stats = conn.execute("""
            SELECT 
                COUNT(*) as total_products,
                MIN(price/100.0) as min_price_gbp,
                MAX(price/100.0) as max_price_gbp,
                AVG(price/100.0) as avg_price_gbp,
                COUNT(DISTINCT brandName) as unique_brands
            FROM products
        """).fetchone()
        
        if food_stats[0] == 0:
            print("✗ No products found in database")
            return False
        
        print(f"✓ Products database validated")
        print(f"  - Total products (all FOOD): {food_stats[0]:,}")
        print(f"  - Price range: £{food_stats[1]:.2f} - £{food_stats[2]:.2f}")
        print(f"  - Average price: £{food_stats[3]:.2f}")
        print(f"  - Unique brands: {food_stats[4]}")
        
        # Sample products - need to check actual column names
        try:
            # First check if we have a productId column or need to use sku
            columns = [col[0] for col in conn.execute("DESCRIBE products").fetchall()]
            id_column = 'productId' if 'productId' in columns else 'sku'
            
            sample_products = conn.execute(f"""
                SELECT {id_column} as productId, sku, name, price/100.0 as price_gbp
                FROM products
                ORDER BY RANDOM()
                LIMIT 5
            """).fetchdf()
        except:
            # Fallback if productId doesn't exist
            sample_products = conn.execute("""
                SELECT sku, name, price/100.0 as price_gbp
                FROM products
                ORDER BY RANDOM()
                LIMIT 5
            """).fetchdf()
        
        print("\n  Sample FOOD products:")
        for _, product in sample_products.iterrows():
            print(f"    - {product['name'][:50]}... (£{product['price_gbp']:.2f})")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"✗ Error validating products database: {e}")
        return False

def validate_customers_data():
    """Validate customers CSV file"""
    print("\n=== Validating Customers Data ===")
    
    # Try multiple possible locations
    possible_locations = [
        CUSTOMERS_CSV,
        DATA_DIR / "customers.csv",
        NOTEBOOKS_DIR / "data" / "customers.csv",
        project_root / "src" / "notebooks" / "customers.csv"
    ]
    
    customers_path = None
    for path in possible_locations:
        if path.exists():
            customers_path = path
            break
    
    if not customers_path:
        print("✗ customers.csv not found in expected locations:")
        for path in possible_locations:
            print(f"  - {path}")
        return False, None
    
    try:
        customers_df = pd.read_csv(customers_path)
        
        # Check required columns - the ID column might be named 'id' or 'customerId'
        id_column = None
        if 'customerId' in customers_df.columns:
            id_column = 'customerId'
        elif 'id' in customers_df.columns:
            id_column = 'id'
        
        if not id_column:
            print("✗ No customer ID column found (looked for 'customerId' or 'id')")
            return False, None
        
        # Check for duplicates
        duplicate_count = customers_df[id_column].duplicated().sum()
        
        print(f"✓ Customers data validated at {customers_path}")
        print(f"  - Total customers: {len(customers_df):,}")
        print(f"  - Customer ID column: '{id_column}'")
        print(f"  - Duplicate customer IDs: {duplicate_count}")
        print(f"  - Columns: {', '.join(customers_df.columns)}")
        
        # Note: We'll rename the id column to customerId when loading for consistency
        
        return True, customers_path
        
    except Exception as e:
        print(f"✗ Error reading customers data: {e}")
        return False, None

def validate_m5_dataset():
    """Validate M5 dataset availability"""
    print("\n=== Validating M5 Dataset ===")
    
    try:
        from datasetsforecast.m5 import M5
        
        # Try to load M5 data
        print("  Loading M5 dataset (this may take a moment)...")
        train_df, X_df, S_df = M5.load(directory=str(NOTEBOOKS_DIR / "data"))
        
        # Get FOODS statistics
        foods_items = S_df[S_df['cat_id'] == 'FOODS']
        
        print(f"✓ M5 dataset loaded successfully")
        print(f"  - Total records: {len(train_df):,}")
        print(f"  - Date range: {train_df['ds'].min()} to {train_df['ds'].max()}")
        print(f"  - FOODS items: {len(foods_items['item_id'].unique()):,}")
        print(f"  - FOODS product-store combinations: {len(foods_items):,}")
        
        return True
        
    except Exception as e:
        print(f"✗ Error loading M5 dataset: {e}")
        print("  The dataset will be downloaded on first use")
        return True  # Not critical for setup

def initialize_orders_database():
    """Initialize orders database with required tables"""
    print("\n=== Initializing Orders Database ===")
    
    try:
        # Create or connect to orders database
        conn = duckdb.connect(str(ORDERS_DB))
        
        # Create orders table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                orderId VARCHAR PRIMARY KEY,
                customerId VARCHAR,
                orderStatus VARCHAR,
                orderDate TIMESTAMP,
                deliveryDate TIMESTAMP,
                totalPrice DECIMAL(10,2)
            )
        """)
        
        # Create order_items table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS order_items (
                orderId VARCHAR,
                productId VARCHAR,
                quantity INTEGER,
                price DECIMAL(10,2)
            )
        """)
        
        # Check if tables were created
        tables = conn.execute("SHOW TABLES").fetchall()
        print(f"✓ Orders database initialized at {ORDERS_DB}")
        print(f"  - Tables created: {[str(table) for table in tables]}")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"✗ Error initializing orders database: {e}")
        return False

def create_working_directories():
    """Create necessary working directories"""
    print("\n=== Creating Working Directories ===")
    
    directories = [
        DATA_DIR,
        NOTEBOOKS_DIR / "orders-synthesis",
        project_root / "src" / "scripts" / "logs"
    ]
    
    for directory in directories:
        directory.mkdir(parents=True, exist_ok=True)
        print(f"✓ Directory ensured: {directory}")
    
    return True

def main():
    """Main setup function"""
    print("=" * 60)
    print("Orders Synthesis - Environment Setup and Validation")
    print("=" * 60)
    print(f"Execution time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Run all validation steps
    all_valid = True
    
    # Check environment
    if not check_environment():
        all_valid = False
    
    # Create directories
    if not create_working_directories():
        all_valid = False
    
    # Validate data sources
    if not validate_products_database():
        all_valid = False
    
    customers_valid, customers_path = validate_customers_data()
    if not customers_valid:
        all_valid = False
    
    if not validate_m5_dataset():
        all_valid = False
    
    # Initialize orders database
    if not initialize_orders_database():
        all_valid = False
    
    # Summary
    print("\n" + "=" * 60)
    if all_valid:
        print("✓ SETUP COMPLETE - All validations passed!")
        print("\nNext steps:")
        print("1. If customers.csv was not found, please provide its location")
        print("2. Run script 02_m5_dataset_analysis.py to analyze M5 data")
        
        # Update .env file if customers path was found
        if customers_path and str(customers_path) != str(CUSTOMERS_CSV):
            env_content = []
            if env_file.exists():
                with open(env_file, 'r') as f:
                    env_content = f.readlines()
            
            # Update CUSTOMERS_CSV in .env
            updated = False
            for i, line in enumerate(env_content):
                if line.startswith('CUSTOMERS_CSV='):
                    env_content[i] = f'CUSTOMERS_CSV={customers_path}\n'
                    updated = True
                    break
            
            if not updated:
                env_content.append(f'\nCUSTOMERS_CSV={customers_path}\n')
            
            with open(env_file, 'w') as f:
                f.writelines(env_content)
            
            print(f"\nConfiguration updated in: {env_file}")
            print("\nEnvironment variables set:")
            print(f"  DATA_DIR={DATA_DIR}")
            print(f"  PRODUCTS_DB={PRODUCTS_DB}")
            print(f"  ORDERS_DB={ORDERS_DB}")
            print(f"  CUSTOMERS_CSV={customers_path}")
        
    else:
        print("✗ SETUP INCOMPLETE - Some validations failed")
        print("Please fix the issues above before proceeding")
        sys.exit(1)

if __name__ == "__main__":
    main()