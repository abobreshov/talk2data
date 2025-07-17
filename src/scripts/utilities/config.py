#!/usr/bin/env python3
"""
Configuration loader for environment variables.
Replaces the old config.json with .env file support.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env file from scripts directory
SCRIPTS_DIR = Path(__file__).parent.parent
ENV_FILE = SCRIPTS_DIR / '.env'

# Load environment variables
if ENV_FILE.exists():
    load_dotenv(ENV_FILE)
else:
    # Try to load from parent directories
    load_dotenv()

# Configuration values with defaults
PRODUCTS_DB = Path(os.getenv('PRODUCTS_DB', SCRIPTS_DIR.parent / 'data' / 'products.duckdb'))
ORDERS_DB = Path(os.getenv('ORDERS_DB', SCRIPTS_DIR.parent / 'data' / 'orders.duckdb'))
CUSTOMERS_CSV = Path(os.getenv('CUSTOMERS_CSV', SCRIPTS_DIR.parent / 'data' / 'customers.csv'))
DATA_DIR = Path(os.getenv('DATA_DIR', SCRIPTS_DIR.parent / 'data'))
SETUP_TIMESTAMP = os.getenv('SETUP_TIMESTAMP', '')

# Ensure data directory exists
DATA_DIR.mkdir(parents=True, exist_ok=True)

def get_config():
    """Get configuration as a dictionary (for compatibility)"""
    return {
        'products_db': str(PRODUCTS_DB),
        'orders_db': str(ORDERS_DB),
        'customers_csv': str(CUSTOMERS_CSV),
        'data_dir': str(DATA_DIR),
        'setup_timestamp': SETUP_TIMESTAMP
    }

def validate_config():
    """Validate that required paths exist"""
    missing = []
    
    if not DATA_DIR.exists():
        missing.append(f"Data directory: {DATA_DIR}")
    
    return missing