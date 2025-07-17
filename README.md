# Grocery POC - E-commerce Data Synthesis System

A comprehensive system for generating realistic e-commerce data combining real product information from UK grocery stores with synthetic order patterns based on Walmart M5 dataset.

## 🎯 Project Overview

This project creates a complete e-commerce dataset including:
- **Real Products**: Scraped from UK grocery stores (currently Aldi)
- **Synthetic Orders**: 2 years of realistic order history for 1,000 customers
- **Complete Supply Chain**: Purchase orders, deliveries, stock levels, and forecasts
- **Analytics Ready**: Pre-built views and reports for business intelligence

This is a comprehensive grocery POC system that includes:
- Data Extraction: TypeScript-based web scraping service for UK grocery stores (currently Aldi)
- Order Synthesis: Python-based system to generate realistic order data using M5 Walmart dataset patterns
- Data Analysis: Jupyter notebooks for data manipulation and analysis using DuckDB

## 🚀 Quick Start

### Prerequisites
- Node.js 18+ and Yarn 4.9.2 (for web scraping)
- Python 3.9+ with Miniconda (for data synthesis)
- ~2GB disk space for generated data

### Environment Setup
```bash
# Clone the repository
git clone <repository-url>
cd grocery_poc

# Setup Python environment
conda create -n grocery_poc python=3.9
conda activate grocery_poc
cd src/scripts
pip install -r requirements.txt

# Setup Node.js environment (for scraping)
cd ../data-extraction
yarn install
```

### Generate Complete Dataset
```bash
cd src/scripts

# Run the complete pipeline
python 00_environment_setup.py
python 10_load_m5_dataset.py
python 12_load_products.py
python 13_create_product_sku_mapping.py
python 20_generate_customers.py
python 21_create_product_mapping.py
python 22_generate_customer_schedules.py
python 30_generate_orders.py
python 32_generate_sales.py
python 33_validate_orders.py
python 40_create_orders_database.py
python 41_create_final_database.py

# Export data
python 90_export_to_parquet.py
python 91_export_to_csv.py
```

Or use the Jupyter notebook:
```bash
cd src/notebooks
jupyter notebook orders-synthesis.ipynb
```

## 📁 Project Structure

```
grocery_poc/
├── src/
│   ├── data-extraction/      # TypeScript web scraper
│   ├── notebooks/            # Jupyter notebooks
│   ├── scripts/              # Python scripts (numbered by function)
│   │   ├── 00-09/           # Environment & Setup
│   │   ├── 10-19/           # Data Loading
│   │   ├── 20-29/           # Customer & Mapping
│   │   ├── 30-39/           # Order Generation
│   │   ├── 40-49/           # Database Creation
│   │   ├── 50-59/           # Supply Chain
│   │   ├── 60-69/           # Inventory Management
│   │   ├── 70-79/           # Forecasting
│   │   ├── 80-89/           # Analytics & Reporting
│   │   ├── 90-99/           # Export & Integration
│   │   └── utilities/       # Helper scripts
│   └── data/                # Generated data
│       ├── grocery_final.db # Main database
│       ├── csv_export/      # CSV files
│       └── parquet_export/  # Parquet files
└── docs/                    # Documentation

```

## 📊 Generated Data

### Scale
- **Products**: 2,501 real products from UK stores
- **Customers**: 1,000 synthetic UK customers
- **Orders**: 131,268 orders over 2 years
- **Order Items**: 1.7M individual items
- **Sales Records**: 2.8M sales transactions

### Key Features
- Realistic order patterns (1-2 orders/week per customer)
- Growing basket values (£36.54 → £40.77 over 2 years)
- 0.5% order cancellation rate
- Proper delivery times (1-7 days)
- Supply chain with late deliveries (8-13%)
- 7-day demand forecasts

## 🔧 Script Categories

### Environment & Setup (00-09)
- `00_environment_setup.py` - Validate environment
- `01_validate_databases.py` - Check data integrity

### Data Loading (10-19)
- `10_load_m5_dataset.py` - Load Walmart M5 data
- `12_load_products.py` - Load scraped products
- `13_create_product_sku_mapping.py` - Generate SKUs

### Customer & Mapping (20-29)
- `20_generate_customers.py` - Load customer data
- `21_create_product_mapping.py` - Map M5 to products
- `22_generate_customer_schedules.py` - Create order schedules

### Order Generation (30-39)
- `30_generate_orders.py` - Generate orders
- `32_generate_sales.py` - Create sales records
- `33_validate_orders.py` - Validate data

### Database Creation (40-49)
- `40_create_orders_database.py` - Create orders DB
- `41_create_final_database.py` - Consolidated database
- `43_create_views.py` - Analytics views

### Supply Chain (50-59)
- `50_create_suppliers.py` - Supplier setup
- `53_create_ordering_calendar.py` - Order windows

### Inventory (60-69)
- `60_generate_initial_stock.py` - Initial stock levels
- `61_generate_purchase_orders.py` - Purchase orders
- `63_generate_realistic_deliveries.py` - Delivery patterns

### Forecasting (70-79)
- `70_generate_forecasts.py` - Demand forecasts
- `72_analyze_forecast_accuracy.py` - Accuracy analysis

### Analytics (80-89)
- `81_create_analytics_views.py` - Analytics views
- `82_generate_reports.py` - Business reports

### Export (90-99)
- `90_export_to_parquet.py` - Parquet export
- `91_export_to_csv.py` - CSV export
- `92_generate_databricks_scripts.py` - Databricks integration

## 📈 Analytics & Reporting

### Pre-built Views
- `customer_analytics` - Customer lifetime value
- `product_performance` - Sales by product
- `stock_availability` - Current inventory
- `forecast_accuracy` - Forecast vs actual
- `supplier_delivery_calendar` - Delivery schedules

### Export Formats
- **DuckDB**: Native analytical database
- **Parquet**: Compressed columnar format
- **CSV**: Universal compatibility
- **Databricks**: SQL scripts for cloud analytics

## 🛠️ Technologies Used

- **Python**: Data synthesis and analytics
  - DuckDB: Embedded analytical database
  - Pandas/NumPy: Data manipulation
  - Matplotlib/Seaborn: Visualizations
  
- **TypeScript**: Web scraping
  - Got-scraping: HTTP with anti-bot protection
  - Joi: Data validation
  - Vitest: Testing framework

- **Jupyter**: Interactive notebooks
- **M5 Dataset**: Walmart sales patterns

## 📝 Documentation

- `CLAUDE.md` - AI assistant guidance
- `docs/` - Detailed documentation
  - `inventory-management-guide.md` - Stock queries
  - `data-regeneration-plan.md` - Regeneration steps
  - `table-usage-review.md` - Database schema

## Common Commands

```bash
# Development (in src/data-extraction/)
yarn install          # Install dependencies (uses Yarn 4.9.2)
yarn build           # Build TypeScript
yarn start           # Run the scraper

# Testing
yarn test            # Run all tests
yarn test:unit       # Run unit tests only (fast, offline)
yarn test:integration # Run integration tests (requires internet)
yarn test:coverage   # Generate coverage report

# API Validation
yarn validate:api    # Validate API response structure

# Notebooks (in src/notebooks/)
# Uses Miniconda environment: ~/miniconda3/envs/grocery_poc
~/miniconda3/envs/grocery_poc/bin/python -m pip install -r requirements.txt  # Install deps
~/miniconda3/envs/grocery_poc/bin/jupyter notebook  # Start Jupyter

# Run complete order synthesis pipeline (in src/scripts/)
~/miniconda3/envs/grocery_poc/bin/python 00_environment_setup.py
~/miniconda3/envs/grocery_poc/bin/python 10_load_m5_dataset.py
~/miniconda3/envs/grocery_poc/bin/python 12_load_products.py
~/miniconda3/envs/grocery_poc/bin/python 13_create_product_sku_mapping.py
~/miniconda3/envs/grocery_poc/bin/python 20_generate_customers.py
~/miniconda3/envs/grocery_poc/bin/python 21_create_product_mapping.py
~/miniconda3/envs/grocery_poc/bin/python 22_generate_customer_schedules.py
~/miniconda3/envs/grocery_poc/bin/python 30_generate_orders.py
~/miniconda3/envs/grocery_poc/bin/python 32_generate_sales.py
~/miniconda3/envs/grocery_poc/bin/python 33_validate_orders.py
~/miniconda3/envs/grocery_poc/bin/python 40_create_orders_database.py
~/miniconda3/envs/grocery_poc/bin/python 41_create_final_database.py
```

## Architecture

**Note**: The project is fully implemented with working scrapers, validation, and testing infrastructure.

### Directory Structure
```
src/
├── data-extraction/   # TypeScript web scraping service
│   ├── aldi/         # Aldi-specific configuration
│   │   └── config/
│   │       └── product-categories.csv
│   ├── src/
│   │   ├── scrapers/ # Store-specific scrapers
│   │   │   └── aldi/
│   │   ├── schemas/  # Joi validation schemas
│   │   └── utils/    # Utility functions
│   ├── tests/        # Unit and integration tests
│   │   ├── unit/
│   │   ├── integration/
│   │   └── fixtures/
│   └── data/         # Output: products.csv
│
├── notebooks/         # Jupyter notebooks
│   ├── orders-synthesis.ipynb        # Main order generation notebook
│   ├── products-skus-synthesis.ipynb # Product-SKU mapping
│   └── playground.ipynb              # Experimentation
│
├── scripts/          # Python scripts organized by function
│   ├── 00-09: Environment & Setup
│   │   ├── 00_environment_setup.py   # Validate environment
│   │   └── 01_validate_databases.py  # Check database integrity
│   ├── 10-19: Data Loading
│   │   ├── 10_load_m5_dataset.py    # Load M5 Walmart data
│   │   ├── 12_load_products.py      # Load products to DB
│   │   └── 13_create_product_sku_mapping.py  # Create SKU mappings
│   ├── 20-29: Customer & Mapping
│   │   ├── 20_generate_customers.py  # Load customer data
│   │   ├── 21_create_product_mapping.py  # Map M5 to products
│   │   └── 22_generate_customer_schedules.py  # Order schedules
│   ├── 30-39: Order Generation
│   │   ├── 30_generate_orders.py     # Generate orders
│   │   ├── 32_generate_sales.py      # Generate sales
│   │   └── 33_validate_orders.py     # Validate data
│   ├── 40-49: Database Creation
│   │   ├── 40_create_orders_database.py  # Create orders DB
│   │   ├── 41_create_final_database.py   # Final consolidated DB
│   │   ├── 42_verify_database.py         # Verify structure
│   │   └── 43_create_views.py            # Create views
│   ├── 50-59: Supply Chain
│   │   ├── 50_create_suppliers.py        # Supplier setup
│   │   ├── 51_create_supplier_schedules.py  # Schedules
│   │   ├── 52_create_product_lifecycle.py   # Expiration data
│   │   └── 53_create_ordering_calendar.py   # Order windows
│   ├── 60-69: Inventory
│   │   ├── 60_generate_initial_stock.py  # Initial stock
│   │   ├── 61_generate_purchase_orders.py # POs
│   │   ├── 62_process_inbound_deliveries.py  # Deliveries
│   │   └── 63_generate_realistic_deliveries.py  # Realistic delivery patterns
│   ├── 70-79: Forecasting
│   │   ├── 70_generate_forecasts.py      # Generate forecasts
│   │   └── 72_analyze_forecast_accuracy.py  # Accuracy metrics
│   ├── 80-89: Analytics
│   │   ├── 80_generate_live_basket.py    # ITB data
│   │   ├── 81_create_analytics_views.py  # Analytics views
│   │   └── 82_generate_reports.py        # Business reports
│   ├── 90-99: Export
│   │   ├── 90_export_to_parquet.py       # Parquet export
│   │   ├── 91_export_to_csv.py           # CSV export
│   │   ├── 92_generate_databricks_scripts.py  # Databricks SQL
│   │   └── 93_export_parquet_usage.py    # Usage examples
│   └── utilities/                        # Helper scripts
│
└── data/             # Data storage
    ├── products.duckdb      # Product catalog with SKU mappings (2,501 products)
    ├── customers.csv        # 1,000 customer records
    ├── orders.db            # Generated orders database
    ├── grocery_final.db     # Consolidated final DuckDB database
    ├── parquet/            # Parquet export directory
    │   ├── *.parquet       # All tables exported as Parquet files
    │   └── export_summary.json
    └── *.pkl               # Intermediate data files

docs/
├── tasks/            # Task definitions and requirements
│   ├── 02-productId-sku-generation.md
│   └── order-synthesis-plan.md
├── products/         # Product data examples and schemas
├── orders/           # Order processing documentation
├── payments/         # Payment processing documentation
├── supply-chain/     # Supply chain logic documentation
└── data-generation/  # Data generation guides
    └── mockaroo-customers.md  # Customer data generation with Mockaroo
```

### Data Generation

#### Customer Data (Mockaroo)
1. **Synthetic Generation**: 1,000 customers created using Mockaroo.com
2. **UK-Focused**: Uses UK postcodes and city names  
3. **Schema**: id, first_name, last_name, email, gender, address, city, postcode
4. **Instructions**: See `docs/data-generation/mockaroo-customers.md` for setup guide

### Key Design Decisions

#### Data Extraction
1. **Store-Specific Architecture**: Each grocery store has its own scraper implementation and configuration directory
2. **CSV-Based Configuration**: Categories loaded from CSV files with ID, name, and URL slug
3. **Rate Limiting**: Random delays (2-10 seconds) between API requests to avoid detection
4. **Progressive Processing**: Fetches 30 products per page, processes categories sequentially
5. **Validation-First**: Uses Joi schemas to validate all API responses before processing

#### Order Synthesis
1. **M5 Dataset Integration**: Uses datasetsforecast.m5 Python package for realistic shopping patterns
2. **Growing Basket Size**: Orders grow from £38 to £43 over 2 years reflecting inflation
3. **Customer Behavior**: 1,000 customers with 1-2 orders/week, 0.5% cancellation rate
4. **Product Mapping**: Weighted mapping between M5 items and actual products based on popularity
5. **Sales Generation**: Sales records created on delivery_date for delivered orders

#### Database Design
1. **ProductId as Primary Key**: Products use productId (18-digit), not SKU
2. **Product-SKU Mapping**: Separate table maps products to multiple warehouse SKUs
3. **Proper Foreign Keys**: All relationships enforced with foreign key constraints
4. **Price Storage**: Prices stored as pence (integers) with virtual GBP column
5. **Analytical Views**: Pre-built views for customer analytics, product performance

## Environment Setup

Create `.env` file from `.env.example`:
```env
ALDI_BASE_URL=https://api.aldi.co.uk/v3/product-search
ALDI_CURRENCY=GBP
ALDI_SERVICE_TYPE=walk-in
ALDI_SORT=relevance
ALDI_TEST_VARIANT=A
ALDI_SERVICE_POINT=C605
ALDI_GET_NOT_FOR_SALE_PRODUCTS=1
```

## Testing Strategy

- **Unit Tests**: Test validation logic with mock data (no network calls)
- **Integration Tests**: Test against live APIs to detect breaking changes
- **API Validator**: Standalone tool to check API contract compliance

Test files:
- `tests/fixtures/valid-response.json` - Expected API response format
- `tests/fixtures/invalid-responses.json` - Examples of broken responses

## Data Flow

### Product Data Extraction
1. Load categories from CSV → 2. Fetch products by category → 3. Validate responses → 4. Transform data → 5. Save to `data/products.csv`

Output format: `sku, name, brandName, sellingSize, currency, price (pence)`

### Order Synthesis Pipeline
1. **Environment Setup**: Validate databases and load customer data
2. **M5 Dataset Loading**: Import Walmart M5 dataset (1,437 FOOD items)
3. **Product Mapping**: Create weighted mapping between M5 items and products
4. **Customer Distribution**: Generate order schedules (131,196 orders over 2 years)
5. **Order Generation**: Create orders with items (1.6M order items)
6. **Sales Generation**: Generate sales records for delivered orders (2.6M sales)
7. **Database Consolidation**: Merge all data into `grocery_final.db`

### Final Database Structure
- **products**: 2,501 products with productId as primary key
- **product_skus**: 5,039 SKU mappings (avg 2 SKUs per product)
- **customers**: 1,000 customer records (includes city field)
- **orders**: 131,268 orders over 2 years
- **order_items**: 1.7M items across all orders
- **sales**: 2.8M sales records (delivered items only)
- **Views**: customer_analytics, product_catalog, product_performance

## Key Dependencies

### Node.js (Data Extraction)
- `got-scraping` - HTTP client with anti-bot protection
- `fast-csv` - CSV processing
- `joi` - Schema validation
- `chalk` - Terminal formatting
- `cli-progress` - Progress tracking
- `vitest` - Testing framework
- `ora` - Terminal spinners
- `boxen` - Terminal boxes
- `dotenv` - Environment configuration

### Python (Order Synthesis & Notebooks)
- `duckdb` - Embedded analytical database
- `pandas` - Data manipulation
- `numpy` - Numerical computing
- `jupyter` - Interactive notebooks
- `datasetsforecast` - M5 Walmart dataset
- `matplotlib` - Plotting (optional)
- `seaborn` - Statistical visualization (optional)
- `tqdm` - Progress bars
- `python-dateutil` - Date utilities
- `colorama` - Terminal colors
- `pyarrow` - Parquet file support

## Python Environment

The project uses Miniconda for Python package management:
- Environment: `~/miniconda3/envs/grocery_poc`
- Python path: `~/miniconda3/envs/grocery_poc/bin/python`
- Notebooks location: `src/notebooks/`

To work with notebooks:
```bash
cd src/notebooks
~/miniconda3/envs/grocery_poc/bin/python -m pip install -r requirements.txt
~/miniconda3/envs/grocery_poc/bin/jupyter notebook
```

## Order Synthesis Scripts

Run the complete order synthesis pipeline:
```bash
cd src/scripts

# Run individual tasks
~/miniconda3/envs/grocery_poc/bin/python 01_environment_setup.py
~/miniconda3/envs/grocery_poc/bin/python 02_load_m5_dataset.py
# ... continue through all scripts

# Or use the integrated notebook
cd ../notebooks
~/miniconda3/envs/grocery_poc/bin/jupyter notebook orders-synthesis.ipynb
```

## Database Queries

Example queries for the final database:
```sql
-- Top products by revenue
SELECT * FROM product_performance 
ORDER BY revenue DESC LIMIT 10;

-- Customer segments
SELECT * FROM customer_analytics 
WHERE lifetime_value > 1000;

-- Product catalog with SKUs
SELECT * FROM product_catalog 
WHERE total_skus > 1;
```

## Data Export

The final database can be exported to Parquet format for integration with other tools:

```bash
# Export to Parquet
~/miniconda3/envs/grocery_poc/bin/python src/scripts/13_export_to_parquet.py

# View usage examples
~/miniconda3/envs/grocery_poc/bin/python src/scripts/14_parquet_usage_examples.py
```

Parquet files are created in `src/data/parquet/` with:
- All 6 tables exported as compressed Parquet files
- All 3 views exported as materialized Parquet files
- Total size: ~70MB (compressed from larger database)
- Compatible with Spark, Pandas, DuckDB, and other analytical tools

## Important Notes

1. **Price Format**: All prices in the database are stored in pence (integers). Use `/100` or the virtual `price_gbp` column for pounds.
2. **Product IDs**: Always use 18-character productId format (padded with zeros)
3. **M5 Dataset**: The M5 dataset is loaded from the `datasetsforecast` Python package, not local files
4. **Order Status**: Orders are marked as DELIVERED, CANCELLED, PICKED, or FUTURE based on dates
5. **Sales Records**: Sales are only created for DELIVERED orders on the delivery_date
6. **Product Count**: The system now includes 2,501 products (updated from original 566)
7. **Customer Schema**: Customer records include city field, ip_address has been removed
8. **Product Coverage**: Order synthesis uses ~54% of available products through improved mapping
9. **Categories**: Products include category and subcategory information from the scraper

## DuckDB Date Handling

### Important: DuckDB Date Functions
When working with dates in DuckDB, use these functions:

```sql
-- Extract date from timestamp (NOT DATE() function!)
DATE_TRUNC('day', timestamp_column)
CAST(timestamp_column AS DATE)

-- Compare dates
WHERE orderDate >= '2025-07-15' AND orderDate < '2025-07-16'

-- Current date
CURRENT_DATE

-- Date arithmetic
date_column + INTERVAL '7 days'
date_column - INTERVAL '2 days'

-- Extract parts
EXTRACT(YEAR FROM date_column)
EXTRACT(MONTH FROM date_column)
EXTRACT(DAY FROM date_column)

-- Format dates
STRFTIME(date_column, '%Y-%m-%d')
```

### Common Mistakes to Avoid
- ❌ `DATE(orderDate)` - This function doesn't exist in DuckDB
- ✓ `DATE_TRUNC('day', orderDate)` - Use this instead
- ✓ `CAST(orderDate AS DATE)` - Alternative approach

## Memory

- Always use DATE_TRUNC or CAST for date extraction in DuckDB, never DATE()
- Data regeneration plan saved in docs/data-regeneration-plan.md
- Current date context: July 14, 2025
- Order status requirements: July 15 = PICKED, July 16-20 = FUTURE

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run validation scripts
5. Submit a pull request

## 📄 License

[Your License Here]

## 🙏 Acknowledgments

- Walmart M5 Competition for sales patterns
- UK grocery stores for product data
- Open source community for tools

