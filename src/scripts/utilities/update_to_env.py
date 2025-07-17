#!/usr/bin/env python3
"""
Utility script to update all scripts from config.json to .env usage
"""

import os
from pathlib import Path
import re

SCRIPTS_DIR = Path(__file__).parent.parent

# Template for config loading replacement
OLD_CONFIG_PATTERN = r"""# Load configuration from previous step
config_path = Path\(__file__\)\.parent / "config\.json"
with open\(config_path, 'r'\) as f:
    config = json\.load\(f\)

DATA_DIR = Path\(config\['data_dir'\]\)"""

NEW_CONFIG_IMPORT = """# Load configuration from environment
import sys
sys.path.append(str(Path(__file__).parent))
from utilities.config import DATA_DIR, PRODUCTS_DB, ORDERS_DB, CUSTOMERS_CSV, get_config

# For compatibility with scripts that use config dict
config = get_config()"""

def update_script(script_path):
    """Update a single script to use .env"""
    with open(script_path, 'r') as f:
        content = f.read()
    
    # Check if it uses config.json
    if "config.json" not in content:
        return False, "No config.json usage found"
    
    # Simple replacements
    replacements = [
        # Remove json import if only used for config
        ("import json\n", ""),
        # Replace config loading block
        ('config_path = Path(__file__).parent / "config.json"', '# Load configuration from environment\nimport sys\nsys.path.append(str(Path(__file__).parent))\nfrom utilities.config import DATA_DIR, PRODUCTS_DB, ORDERS_DB, CUSTOMERS_CSV, get_config'),
        ('with open(config_path, \'r\') as f:\n    config = json.load(f)', 'config = get_config()'),
        ('DATA_DIR = Path(config[\'data_dir\'])', '# DATA_DIR already imported from config'),
        ('PRODUCTS_DB = Path(config[\'products_db\'])', '# PRODUCTS_DB already imported from config'),
        ('ORDERS_DB = Path(config[\'orders_db\'])', '# ORDERS_DB already imported from config'),
        ('CUSTOMERS_CSV = Path(config[\'customers_csv\'])', '# CUSTOMERS_CSV already imported from config'),
    ]
    
    modified = False
    for old, new in replacements:
        if old in content:
            content = content.replace(old, new)
            modified = True
    
    if modified:
        # Clean up duplicate imports
        lines = content.split('\n')
        seen_imports = set()
        cleaned_lines = []
        
        for line in lines:
            if line.strip().startswith('import ') or line.strip().startswith('from '):
                if line in seen_imports:
                    continue
                seen_imports.add(line)
            cleaned_lines.append(line)
        
        content = '\n'.join(cleaned_lines)
        
        # Save updated content
        with open(script_path, 'w') as f:
            f.write(content)
        
        return True, "Updated successfully"
    
    return False, "Could not update automatically"

def main():
    """Update all scripts"""
    scripts_to_update = [
        '10_load_m5_dataset.py',
        '20_generate_customers.py', 
        '21_create_product_mapping.py',
        '22_generate_customer_schedules.py',
        '30_generate_orders.py',
        '32_generate_sales.py',
        '33_validate_orders.py',
        '40_create_orders_database.py',
        '41_create_final_database.py',
    ]
    
    print("Updating scripts to use .env instead of config.json...")
    
    for script_name in scripts_to_update:
        script_path = SCRIPTS_DIR / script_name
        if script_path.exists():
            success, message = update_script(script_path)
            status = "✓" if success else "✗"
            print(f"{status} {script_name}: {message}")
        else:
            print(f"⚠ {script_name}: File not found")

if __name__ == "__main__":
    main()