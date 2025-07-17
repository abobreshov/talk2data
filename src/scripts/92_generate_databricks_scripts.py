#!/usr/bin/env python3
"""
Generate SQL statements to create views in Databricks.
Fixed version that properly handles DuckDB to Databricks SQL conversion.
"""

import duckdb
from pathlib import Path
from datetime import datetime
import re

# Database path
DB_PATH = Path(__file__).parent.parent / 'data' / 'grocery_final.db'
OUTPUT_PATH = Path(__file__).parent.parent / 'data' / 'databricks_views_fixed.sql'

def clean_view_definition(view_def):
    """Clean and format view definition for better readability"""
    # Remove CREATE VIEW statement to get just the SELECT
    view_def = re.sub(r'^CREATE VIEW \w+ AS\s+', '', view_def, flags=re.IGNORECASE)
    
    # Clean up excessive whitespace first
    view_def = re.sub(r'\s+', ' ', view_def)
    
    # Add newlines before major clauses for readability
    major_clauses = ['FROM', 'WHERE', 'GROUP BY', 'ORDER BY', 'HAVING', 'LEFT JOIN', 'INNER JOIN', 'RIGHT JOIN']
    for clause in major_clauses:
        view_def = re.sub(f'\\s+{clause}\\s+', f'\n{clause} ', view_def, flags=re.IGNORECASE)
    
    return view_def.strip()

def convert_to_databricks_sql(view_name, view_def):
    """Convert DuckDB view definition to Databricks SQL"""
    # Clean the definition first
    clean_def = clean_view_definition(view_def)
    
    # Special handling for specific problematic views
    if view_name == 'latest_forecasts':
        # Rewrite the complex row comparison
        clean_def = """
SELECT f.*
FROM forecasts f
INNER JOIN (
    SELECT productId, MAX(forecast_date) as max_forecast_date
    FROM forecasts
    GROUP BY productId
) latest ON f.productId = latest.productId AND f.forecast_date = latest.max_forecast_date"""
    
    # DuckDB to Databricks SQL conversions
    conversions = {
        # Remove "row" function syntax
        r'"row"\([^)]+\)\s*=\s*ANY\s*\([^)]+\)': '',
        
        # Date/time functions
        r'CURRENT_DATE(?!\(\))': 'CURRENT_DATE()',
        r'CURRENT_TIMESTAMP(?!\(\))': 'CURRENT_TIMESTAMP()',
        r'DATE_TRUNC\(\'(\w+)\',\s*([^)]+)\)': r'DATE_TRUNC("\1", \2)',
        r'date_part\(\'DOW\',\s*([^)]+)\)': r'DAYOFWEEK(\1)',
        
        # CAST expressions
        r'CAST\s*\(\s*\'([tf])\'\s*AS\s+BOOLEAN\s*\)': lambda m: 'TRUE' if m.group(1) == 't' else 'FALSE',
        r'CAST\s*\(\s*\'(\d+)\s+days?\'\s*AS\s+INTERVAL\s*\)': r'INTERVAL \1 DAYS',
        r'CAST\s*\(\s*\'(\d+)\s+hours?\'\s*AS\s+INTERVAL\s*\)': r'INTERVAL \1 HOURS',
        
        # String functions
        r'SUBSTRING\(([^,]+)\s+FROM\s+(\d+)\s+FOR\s+(\d+)\)': r'SUBSTRING(\1, \2, \3)',
        r'SUBSTRING\(([^,]+)\s+FROM\s+(\d+)\)': r'SUBSTRING(\1, \2)',
        
        # Remove schema prefixes
        r'main\.': '',
        
        # Fix quoted identifiers
        r'"name"': '`name`',
        r'"row"': '`row`',
        
        # Remove type casts that Databricks handles automatically
        r'::DECIMAL\(?\d*,?\d*\)?': '',
        r'::INTEGER': '',
        r'::BIGINT': '',
        r'::VARCHAR': '',
        r'::DATE': '',
        r'::TIMESTAMP': '',
        r'::BOOLEAN': '',
        
        # Fix double semicolons
        r';;': ';',
    }
    
    # Apply conversions
    databricks_def = clean_def
    for pattern, replacement in conversions.items():
        if callable(replacement):
            databricks_def = re.sub(pattern, replacement, databricks_def, flags=re.IGNORECASE)
        else:
            databricks_def = re.sub(pattern, replacement, databricks_def, flags=re.IGNORECASE)
    
    # Fix DAYOFWEEK mapping (DuckDB uses 0-6 starting Sunday, Databricks uses 1-7 starting Sunday)
    if 'DAYOFWEEK' in databricks_def and 'delivery_day_of_week' in databricks_def:
        # Replace the CASE statement for day of week
        databricks_def = re.sub(
            r'CASE WHEN \(\(DAYOFWEEK\([^)]+\) = 1\)\) THEN \(\'Monday\'\).*?ELSE NULL END',
            """CASE 
        WHEN DAYOFWEEK(ss.delivery_date) = 2 THEN 'Monday'
        WHEN DAYOFWEEK(ss.delivery_date) = 3 THEN 'Tuesday'
        WHEN DAYOFWEEK(ss.delivery_date) = 4 THEN 'Wednesday'
        WHEN DAYOFWEEK(ss.delivery_date) = 5 THEN 'Thursday'
        WHEN DAYOFWEEK(ss.delivery_date) = 6 THEN 'Friday'
        ELSE NULL 
    END""",
            databricks_def,
            flags=re.IGNORECASE | re.DOTALL
        )
    
    # Create the full CREATE VIEW statement
    return f"CREATE OR REPLACE VIEW {view_name} AS\n{databricks_def}"

def generate_databricks_views():
    """Generate Databricks-compatible view definitions"""
    print("=== Generating Fixed Databricks View Definitions ===\n")
    
    try:
        # Connect to database
        conn = duckdb.connect(str(DB_PATH))
        
        # Get all views
        views = conn.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_type = 'VIEW'
            ORDER BY table_name
        """).fetchall()
        
        print(f"Found {len(views)} views to convert\n")
        
        # Collect all view definitions
        all_views_sql = []
        view_details = []
        
        # Add header
        header = f"""-- Databricks View Definitions (Fixed Version)
-- Generated from: {DB_PATH}
-- Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
-- Total views: {len(views)}

-- INSTRUCTIONS:
-- 1. Ensure all tables are loaded in Databricks (use Parquet files)
-- 2. Update the catalog and schema names below
-- 3. Run this script in Databricks SQL
-- 4. All syntax has been converted from DuckDB to Databricks SQL

-- Update these with your actual catalog and schema names:
USE CATALOG workspace;    -- Your catalog name
USE SCHEMA default;       -- Your schema name

"""
        all_views_sql.append(header)
        
        # Process each view
        for (view_name,) in views:
            print(f"Processing view: {view_name}")
            
            # Get view definition from DuckDB
            try:
                view_def = conn.execute(f"""
                    SELECT sql 
                    FROM sqlite_master 
                    WHERE type = 'view' 
                    AND name = '{view_name}'
                """).fetchone()
                
                if view_def and view_def[0]:
                    view_def = view_def[0]
                else:
                    continue
            except:
                print(f"  ⚠️  Could not retrieve definition for view: {view_name}")
                continue
            
            # Convert to Databricks SQL
            databricks_sql = convert_to_databricks_sql(view_name, view_def)
            
            # Add to collection with proper formatting
            all_views_sql.append(f"\n-- =====================================================")
            all_views_sql.append(f"-- View: {view_name}")
            all_views_sql.append(f"-- =====================================================\n")
            all_views_sql.append(databricks_sql + ";\n")
            
            # Get row count for documentation
            try:
                row_count = conn.execute(f"SELECT COUNT(*) FROM {view_name}").fetchone()[0]
                view_details.append((view_name, row_count))
                print(f"  ✓ Converted successfully (rows: {row_count:,})")
            except:
                view_details.append((view_name, 'N/A'))
                print(f"  ✓ Converted successfully")
        
        # Add verification queries at the end
        verification = f"""
-- =====================================================
-- Verification Queries
-- =====================================================
-- Run these after creating all views to verify they work:

"""
        for view_name, row_count in view_details:
            verification += f"-- SELECT COUNT(*) FROM {view_name};  -- Expected: {row_count}\n"
        
        all_views_sql.append(verification)
        
        # Write to file
        with open(OUTPUT_PATH, 'w') as f:
            f.write('\n'.join(all_views_sql))
        
        print(f"\n✓ Generated fixed Databricks view definitions")
        print(f"  Output file: {OUTPUT_PATH}")
        print(f"  Total views: {len(views)}")
        
        # Create individual view files for easier debugging
        individual_dir = Path(OUTPUT_PATH).parent / 'databricks_views'
        individual_dir.mkdir(exist_ok=True)
        
        # Parse and save individual views
        current_view = None
        current_sql = []
        
        for line in all_views_sql:
            if '-- View:' in line and '====' in all_views_sql[all_views_sql.index(line) - 1]:
                # Save previous view if exists
                if current_view and current_sql:
                    view_file = individual_dir / f"{current_view}.sql"
                    with open(view_file, 'w') as f:
                        f.write('\n'.join(current_sql))
                
                # Start new view
                current_view = line.split('-- View:')[1].strip()
                current_sql = [f"-- {current_view}.sql\n-- Individual view definition for Databricks\n"]
                current_sql.append(f"USE CATALOG workspace;  -- Update with your catalog")
                current_sql.append(f"USE SCHEMA default;    -- Update with your schema\n")
            elif current_view:
                current_sql.append(line)
        
        # Save last view
        if current_view and current_sql:
            view_file = individual_dir / f"{current_view}.sql"
            with open(view_file, 'w') as f:
                f.write('\n'.join(current_sql))
        
        print(f"  Individual view files: {individual_dir}")
        
        conn.close()
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        raise

if __name__ == "__main__":
    generate_databricks_views()