#!/usr/bin/env python3
"""
Create product expiration reference table with shelf life periods by category.

This table defines average shelf life in days for different product categories,
helping with inventory management and product purging decisions.
"""

import duckdb
from pathlib import Path
import json
from datetime import datetime
from colorama import init, Fore, Style

init(autoreset=True)

# Define shelf life periods by category/subcategory
# Based on UK food safety standards and typical retail practices
SHELF_LIFE_REFERENCE = {
    # Fresh Food - Short shelf life
    "Fresh Food": {
        "default": 7,
        "subcategories": {
            "Vegetables": 7,
            "Fruit": 7,
            "Herbs & Spices": 10,  # Fresh herbs
            "Salad": 5,
            "Fresh Soup": 3,
        }
    },
    
    # Chilled Food - Medium shelf life
    "Chilled Food": {
        "default": 10,
        "subcategories": {
            "Chilled Meats": 5,
            "Poultry": 3,
            "Beef": 5,
            "Pork": 5,
            "Lamb": 5,
            "Fish & Seafood": 2,
            "Cheese": 21,
            "Milk & Cream": 14,
            "Yogurt & Desserts": 14,
            "Chilled Desserts": 7,
            "Ready Meals": 5,
            "Food To Go": 3,
            "Party Food, Pies & Salads": 5,
            "Eggs": 28,
            "Butter & Spreads": 30,
            "Dips & Pâté": 7,
            "Fresh Pasta & Sauce": 7,
            "Cooked Meats": 5,
        }
    },
    
    # Bakery - Very short shelf life
    "Bakery": {
        "default": 3,
        "subcategories": {
            "Bread & Rolls": 3,
            "Cakes & Treats": 5,
            "Pastries": 2,
            "Fresh Bakery": 2,
            "Part Baked": 7,
        }
    },
    
    # Food Cupboard - Long shelf life
    "Food Cupboard": {
        "default": 365,
        "subcategories": {
            "Chocolate & Sweets": 365,
            "Biscuits & Crackers": 180,
            "Crisps & Snacks": 120,
            "Cereals & Snack Bars": 365,
            "Tins, Cans & Packets": 730,  # 2 years
            "Rice, Pasta & Noodles": 730,
            "Sauces, Oils & Dressings": 365,
            "Jams, Spreads & Syrups": 365,
            "Seeds, Nuts & Dried Fruits": 180,
            "Tea & Coffee": 365,
            "Soft Drinks": 270,
            "Alcohol": 1095,  # 3 years
            "Baby Food": 365,
            "Pet Food": 365,
            "Flour & Baking": 365,
            "Sugar & Sweeteners": 730,
            "Herbs & Spices": 730,  # Dried
            "Condiments": 365,
            "Cooking Sauces": 365,
            "Stock & Gravy": 365,
        }
    },
    
    # Generic Food category
    "Food": {
        "default": 90,
        "subcategories": {}
    }
}

def get_shelf_life(category, subcategory):
    """Get shelf life for a specific category/subcategory combination."""
    if category in SHELF_LIFE_REFERENCE:
        cat_ref = SHELF_LIFE_REFERENCE[category]
        # Check if subcategory has specific shelf life
        if subcategory and subcategory in cat_ref["subcategories"]:
            return cat_ref["subcategories"][subcategory]
        # Return category default
        return cat_ref["default"]
    # Return general default
    return 30

def create_product_purge_reference():
    """Create and populate the product purge reference table."""
    print(f"\n{Fore.CYAN}{'='*60}")
    print(f"{Fore.CYAN}Creating Product Expiration Reference Table")
    print(f"{Fore.CYAN}{'='*60}\n")
    
    # Connect to database
    db_path = Path(__file__).parent.parent / "data" / "grocery_final.db"
    conn = duckdb.connect(str(db_path))
    
    # Get unique category/subcategory combinations
    print(f"{Fore.YELLOW}Analyzing product categories...")
    cat_combinations = conn.execute("""
        SELECT DISTINCT 
            category,
            subcategory,
            COUNT(*) as product_count
        FROM products
        WHERE category IS NOT NULL
        GROUP BY category, subcategory
        ORDER BY category, subcategory
    """).df()
    
    print(f"{Fore.GREEN}✓ Found {len(cat_combinations)} unique category/subcategory combinations")
    
    # Create reference data
    reference_data = []
    
    for _, row in cat_combinations.iterrows():
        category = row['category']
        subcategory = row['subcategory']
        product_count = row['product_count']
        
        shelf_life_days = get_shelf_life(category, subcategory)
        
        reference_data.append({
            'category': category,
            'subcategory': subcategory if subcategory else None,
            'shelf_life_days': shelf_life_days,
            'min_shelf_life_days': int(shelf_life_days * 0.8),  # 80% of typical
            'max_shelf_life_days': int(shelf_life_days * 1.2),  # 120% of typical
            'temperature_sensitive': category in ['Fresh Food', 'Chilled Food', 'Bakery'],
            'requires_refrigeration': category == 'Chilled Food',
            'product_count': product_count,
            'notes': None
        })
    
    # Add specific notes for certain categories
    for ref in reference_data:
        if ref['subcategory'] == 'Milk & Cream':
            ref['notes'] = 'Use within 3 days of opening'
        elif ref['subcategory'] == 'Fish & Seafood':
            ref['notes'] = 'Consume on day of purchase for best quality'
        elif ref['subcategory'] == 'Bread & Rolls':
            ref['notes'] = 'Can be frozen to extend shelf life'
        elif ref['subcategory'] in ['Beef', 'Pork', 'Lamb', 'Poultry']:
            ref['notes'] = 'Can be frozen before use-by date'
        elif ref['subcategory'] == 'Eggs':
            ref['notes'] = 'Best before date typically 28 days from laying'
    
    # Create DataFrame
    import pandas as pd
    reference_df = pd.DataFrame(reference_data)
    
    # Drop and create table
    print(f"\n{Fore.CYAN}Creating product_purge_reference table...")
    
    conn.execute("DROP TABLE IF EXISTS product_purge_reference")
    conn.execute("DROP SEQUENCE IF EXISTS product_purge_reference_seq")
    
    conn.execute("""
        CREATE SEQUENCE product_purge_reference_seq;
        CREATE TABLE product_purge_reference (
            reference_id INTEGER PRIMARY KEY DEFAULT nextval('product_purge_reference_seq'),
            category VARCHAR NOT NULL,
            subcategory VARCHAR,
            shelf_life_days INTEGER NOT NULL,
            min_shelf_life_days INTEGER NOT NULL,
            max_shelf_life_days INTEGER NOT NULL,
            temperature_sensitive BOOLEAN DEFAULT FALSE,
            requires_refrigeration BOOLEAN DEFAULT FALSE,
            product_count INTEGER,
            notes VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Insert data
    conn.execute("""
        INSERT INTO product_purge_reference 
        (category, subcategory, shelf_life_days, min_shelf_life_days, max_shelf_life_days,
         temperature_sensitive, requires_refrigeration, product_count, notes)
        SELECT * FROM reference_df
    """)
    
    # Create indexes
    conn.execute("CREATE INDEX idx_purge_ref_category ON product_purge_reference(category)")
    conn.execute("CREATE INDEX idx_purge_ref_subcategory ON product_purge_reference(subcategory)")
    
    # Create useful view
    conn.execute("""
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
            AND (p.subcategory = ppr.subcategory OR (p.subcategory IS NULL AND ppr.subcategory IS NULL))
        ORDER BY ppr.shelf_life_days, p.category, p.subcategory
    """)
    
    print(f"{Fore.GREEN}✓ Created view: product_expiration_guide")
    
    # Show summary statistics
    print(f"\n{Fore.YELLOW}Shelf Life Summary by Category:")
    summary = conn.execute("""
        SELECT 
            category,
            COUNT(DISTINCT subcategory) as subcategory_count,
            MIN(shelf_life_days) as min_days,
            MAX(shelf_life_days) as max_days,
            ROUND(AVG(shelf_life_days), 1) as avg_days,
            SUM(product_count) as total_products
        FROM product_purge_reference
        GROUP BY category
        ORDER BY avg_days
    """).df()
    
    print(summary.to_string(index=False))
    
    # Show examples
    print(f"\n{Fore.YELLOW}Example Shelf Life by Subcategory:")
    examples = conn.execute("""
        SELECT 
            category,
            subcategory,
            shelf_life_days,
            CASE 
                WHEN requires_refrigeration THEN 'Yes' 
                ELSE 'No' 
            END as refrigeration,
            product_count
        FROM product_purge_reference
        WHERE subcategory IN ('Milk & Cream', 'Beef', 'Vegetables', 'Bread & Rolls', 
                              'Tins, Cans & Packets', 'Chocolate & Sweets')
        ORDER BY shelf_life_days
    """).df()
    
    print(examples.to_string(index=False))
    
    # Save summary
    total_references = conn.execute("SELECT COUNT(*) FROM product_purge_reference").fetchone()[0]
    
    summary_data = {
        'total_references': total_references,
        'categories': len(summary),
        'shelf_life_ranges': {
            'very_short': len(reference_df[reference_df['shelf_life_days'] <= 3]),
            'short': len(reference_df[(reference_df['shelf_life_days'] > 3) & (reference_df['shelf_life_days'] <= 7)]),
            'medium': len(reference_df[(reference_df['shelf_life_days'] > 7) & (reference_df['shelf_life_days'] <= 30)]),
            'long': len(reference_df[(reference_df['shelf_life_days'] > 30) & (reference_df['shelf_life_days'] <= 365)]),
            'very_long': len(reference_df[reference_df['shelf_life_days'] > 365])
        },
        'temperature_sensitive_count': len(reference_df[reference_df['temperature_sensitive']]),
        'requires_refrigeration_count': len(reference_df[reference_df['requires_refrigeration']]),
        'timestamp': datetime.now().isoformat()
    }
    
    summary_path = Path(__file__).parent.parent / 'data' / 'product_purge_reference_summary.json'
    with open(summary_path, 'w') as f:
        json.dump(summary_data, f, indent=2)
    
    print(f"\n{Fore.GREEN}✓ Summary saved to product_purge_reference_summary.json")
    
    conn.close()
    print(f"\n{Fore.CYAN}{'='*60}")
    print(f"{Fore.GREEN}✓ Product expiration reference table created successfully!")
    print(f"{Fore.CYAN}{'='*60}\n")

if __name__ == "__main__":
    create_product_purge_reference()