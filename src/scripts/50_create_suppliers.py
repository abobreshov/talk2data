#!/usr/bin/env python3
"""
Create suppliers table in grocery_final.db from unique brands in products table.

This script:
1. Extracts unique brands from the products table
2. Generates numeric supplier IDs
3. Creates a suppliers table
4. Updates products table with supplier_id foreign key
"""

import duckdb
import os
from datetime import datetime
from colorama import Fore, Style, init

# Initialize colorama for Windows support
init(autoreset=True)

# Constants
DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'grocery_final.db')

def print_header(message):
    """Print colored header message."""
    print(f"\n{Fore.CYAN}{'='*60}")
    print(f"{Fore.CYAN}{message}")
    print(f"{Fore.CYAN}{'='*60}{Style.RESET_ALL}")

def print_success(message):
    """Print success message in green."""
    print(f"{Fore.GREEN}✓ {message}{Style.RESET_ALL}")

def print_info(message):
    """Print info message in yellow."""
    print(f"{Fore.YELLOW}ℹ {message}{Style.RESET_ALL}")

def print_error(message):
    """Print error message in red."""
    print(f"{Fore.RED}✗ {message}{Style.RESET_ALL}")

def create_suppliers_table():
    """Create suppliers table from unique brands in products table."""
    
    print_header("Creating Suppliers Table")
    
    # Check if database exists
    if not os.path.exists(DB_PATH):
        print_error(f"Database not found at {DB_PATH}")
        return False
    
    try:
        # Connect to database
        conn = duckdb.connect(DB_PATH)
        
        # Get unique brands from products table
        print_info("Extracting unique brands from products table...")
        
        unique_brands_query = """
        SELECT DISTINCT brandName
        FROM products
        WHERE brandName IS NOT NULL AND brandName != ''
        ORDER BY brandName
        """
        
        brands_df = conn.execute(unique_brands_query).df()
        num_brands = len(brands_df)
        
        print_success(f"Found {num_brands} unique brands")
        
        # Create suppliers table
        print_info("Creating suppliers table...")
        
        # Drop table if exists
        conn.execute("DROP TABLE IF EXISTS suppliers CASCADE")
        
        # Create suppliers table
        create_table_query = """
        CREATE TABLE suppliers (
            supplier_id INTEGER PRIMARY KEY,
            supplier_name VARCHAR NOT NULL,
            contact_email VARCHAR,
            contact_phone VARCHAR,
            address VARCHAR,
            city VARCHAR,
            postcode VARCHAR,
            country VARCHAR DEFAULT 'UK',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        
        conn.execute(create_table_query)
        print_success("Suppliers table created")
        
        # Generate supplier data
        print_info("Generating supplier records...")
        
        suppliers_data = []
        for idx, row in brands_df.iterrows():
            brand_name = row['brandName']
            supplier_id = idx + 1
            
            # Generate email and phone based on brand name
            # Clean brand name for email
            email_base = brand_name.lower().replace(' ', '.').replace('&', 'and').replace("'", '')
            email = f"supplier@{email_base}.co.uk"
            
            # Generate phone number (UK format)
            phone = f"0{1000 + supplier_id} {555000 + supplier_id}"
            
            # Generate address
            address = f"{supplier_id} {brand_name} House, Business Park"
            
            # Rotate through some UK cities
            cities = ['London', 'Manchester', 'Birmingham', 'Leeds', 'Glasgow', 
                     'Liverpool', 'Bristol', 'Sheffield', 'Edinburgh', 'Cardiff']
            city = cities[idx % len(cities)]
            
            # Generate postcode
            postcode = f"{city[:2].upper()}{(idx % 9) + 1} {(idx % 9) + 1}AB"
            
            suppliers_data.append({
                'supplier_id': supplier_id,
                'supplier_name': brand_name,
                'contact_email': email,
                'contact_phone': phone,
                'address': address,
                'city': city,
                'postcode': postcode,
                'country': 'UK'
            })
        
        # Insert suppliers data
        insert_query = """
        INSERT INTO suppliers (
            supplier_id, supplier_name, contact_email, contact_phone,
            address, city, postcode, country
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        for supplier in suppliers_data:
            conn.execute(insert_query, [
                supplier['supplier_id'],
                supplier['supplier_name'],
                supplier['contact_email'],
                supplier['contact_phone'],
                supplier['address'],
                supplier['city'],
                supplier['postcode'],
                supplier['country']
            ])
        
        print_success(f"Inserted {len(suppliers_data)} supplier records")
        
        # Add supplier_id column to products table
        print_info("Adding supplier_id column to products table...")
        
        # Check if column already exists
        columns_query = """
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = 'products' AND column_name = 'supplier_id'
        """
        
        existing_column = conn.execute(columns_query).fetchone()
        
        if not existing_column:
            # Add supplier_id column
            conn.execute("ALTER TABLE products ADD COLUMN supplier_id INTEGER")
            print_success("Added supplier_id column to products table")
        else:
            print_info("supplier_id column already exists in products table")
        
        # Update products with supplier_id based on brand mapping
        print_info("Updating products with supplier_id...")
        
        update_query = """
        UPDATE products
        SET supplier_id = s.supplier_id
        FROM suppliers s
        WHERE products.brandName = s.supplier_name
        """
        
        result = conn.execute(update_query)
        print_success(f"Updated products with supplier_id")
        
        # Add foreign key constraint
        print_info("Adding foreign key constraint...")
        
        # Note: DuckDB doesn't enforce foreign keys but we can add the constraint for documentation
        fk_query = """
        ALTER TABLE products 
        ADD CONSTRAINT fk_products_supplier 
        FOREIGN KEY (supplier_id) REFERENCES suppliers(supplier_id)
        """
        
        try:
            conn.execute(fk_query)
            print_success("Added foreign key constraint")
        except:
            print_info("Foreign key constraint already exists or not supported")
        
        # Verify the results
        print_header("Verification")
        
        # Count suppliers
        supplier_count = conn.execute("SELECT COUNT(*) FROM suppliers").fetchone()[0]
        print_info(f"Total suppliers: {supplier_count}")
        
        # Show sample suppliers
        print_info("\nSample suppliers:")
        sample_suppliers = conn.execute("""
            SELECT supplier_id, supplier_name, city, contact_email
            FROM suppliers
            ORDER BY supplier_id
            LIMIT 5
        """).df()
        
        for _, row in sample_suppliers.iterrows():
            print(f"  {row['supplier_id']}: {row['supplier_name']} ({row['city']}) - {row['contact_email']}")
        
        # Count products with supplier_id
        products_with_supplier = conn.execute("""
            SELECT COUNT(*) 
            FROM products 
            WHERE supplier_id IS NOT NULL
        """).fetchone()[0]
        
        total_products = conn.execute("SELECT COUNT(*) FROM products").fetchone()[0]
        
        print_info(f"\nProducts with supplier_id: {products_with_supplier}/{total_products}")
        
        # Show products per supplier (top 5)
        print_info("\nTop 5 suppliers by product count:")
        top_suppliers = conn.execute("""
            SELECT s.supplier_name, COUNT(p.productId) as product_count
            FROM suppliers s
            LEFT JOIN products p ON s.supplier_id = p.supplier_id
            GROUP BY s.supplier_name
            ORDER BY product_count DESC
            LIMIT 5
        """).df()
        
        for _, row in top_suppliers.iterrows():
            print(f"  {row['supplier_name']}: {row['product_count']} products")
        
        # Commit changes
        conn.commit()
        conn.close()
        
        print_header("Suppliers Table Created Successfully")
        
        return True
        
    except Exception as e:
        print_error(f"Error creating suppliers table: {str(e)}")
        return False

def main():
    """Main function."""
    print_header("Supplier Table Generation Script")
    print_info(f"Database: {DB_PATH}")
    print_info(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    success = create_suppliers_table()
    
    if success:
        print_success(f"\nCompleted at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    else:
        print_error("\nScript failed!")
        exit(1)

if __name__ == "__main__":
    main()