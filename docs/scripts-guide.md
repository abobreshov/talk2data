# Scripts Organization Guide

## Overview

The scripts have been reorganized into functional groups with sequential numbering (00-99) for better clarity and execution order.

## Script Categories

### 00-09: Environment & Setup
Scripts for environment validation and initial setup.

- `00_environment_setup.py` - Validates Python environment and dependencies
- `01_validate_databases.py` - Checks database integrity and structure

### 10-19: Data Loading
Scripts for loading external data sources.

- `10_load_m5_dataset.py` - Loads Walmart M5 dataset for shopping patterns
- `12_load_products.py` - Loads scraped product data
- `13_create_product_sku_mapping.py` - Generates SKU mappings for products

### 20-29: Customer & Mapping
Scripts for customer data and product mapping.

- `20_generate_customers.py` - Loads/processes customer data
- `21_create_product_mapping.py` - Maps M5 items to actual products
- `22_generate_customer_schedules.py` - Creates order schedules for customers

### 30-39: Order Generation
Core order synthesis and validation.

- `30_generate_orders.py` - Generates customer orders
- `32_generate_sales.py` - Creates sales records from orders
- `33_validate_orders.py` - Validates generated order data

### 40-49: Database Creation
Database consolidation and view creation.

- `40_create_orders_database.py` - Creates orders database
- `41_create_final_database.py` - Consolidates all data into final DB
- `43_create_views.py` - Creates analytical views

### 50-59: Supply Chain
Supplier and procurement management.

- `50_create_suppliers.py` - Sets up supplier data
- `51_generate_supplier_schedules.py` - Creates delivery schedules
- `52_create_supplier_product_links.py` - Links products to suppliers
- `53_create_ordering_calendar.py` - Creates ordering windows

### 60-69: Inventory Management
Stock levels and purchase order generation.

- `60_generate_initial_stock.py` - Sets initial stock levels
- `61_generate_purchase_orders.py` - Creates purchase orders
- `62_process_deliveries.py` - Processes inbound deliveries
- `63_generate_realistic_deliveries.py` - Simulates real delivery patterns
- `64_update_stock_from_deliveries.py` - Updates stock from deliveries
- `65_generate_live_basket.py` - Creates in-the-basket data

### 70-79: Forecasting
Demand forecasting and accuracy analysis.

- `70_generate_forecasts.py` - Creates demand forecasts
- `71_generate_future_forecasts.py` - Forecasts for future periods
- `72_analyze_forecast_accuracy.py` - Analyzes forecast performance

### 80-89: Analytics & Reporting
Business analytics and reporting views.

- `81_create_analytics_views.py` - Advanced analytical views
- `82_generate_reports.py` - Business reports and visualizations

### 90-99: Export & Integration
Data export and external system integration.

- `90_export_to_parquet.py` - Export all tables to Parquet format
- `91_export_to_csv.py` - Export all tables to CSV format
- `92_generate_databricks_scripts.py` - Generate Databricks SQL scripts

### Utilities
Helper scripts and tools in `utilities/` subdirectory.

## Execution Order

### Basic Order Synthesis Pipeline
```bash
# Core pipeline for generating orders
python 00_environment_setup.py
python 10_load_m5_dataset.py
python 12_load_products.py
python 20_generate_customers.py
python 21_create_product_mapping.py
python 22_generate_customer_schedules.py
python 30_generate_orders.py
python 32_generate_sales.py
python 40_create_orders_database.py
python 41_create_final_database.py
```

### Full Pipeline with Supply Chain
```bash
# Complete pipeline including inventory management
# Run basic pipeline first, then:
python 50_create_suppliers.py
python 51_generate_supplier_schedules.py
python 53_create_ordering_calendar.py
python 60_generate_initial_stock.py
python 61_generate_purchase_orders.py
python 70_generate_forecasts.py
python 81_create_analytics_views.py
python 90_export_to_parquet.py
```

## Migration Notes

### Old to New Script Mapping

| Old Name | New Name | Category |
|----------|----------|----------|
| 01_environment_setup.py | 00_environment_setup.py | Setup |
| 02_m5_dataset_analysis.py | 10_load_m5_dataset.py | Data Loading |
| 03_product_mapping.py | 21_create_product_mapping.py | Mapping |
| 04_customer_distribution.py | 22_generate_customer_schedules.py | Customer |
| 05_order_generation.py | 30_generate_orders.py | Orders |
| 06_sales_generation.py | 32_generate_sales.py | Orders |
| 07_final_validation.py | 33_validate_orders.py | Orders |
| 08_load_to_database.py | 40_create_orders_database.py | Database |
| 11_create_final_database.py | 41_create_final_database.py | Database |
| 15_create_suppliers_table.py | 50_create_suppliers.py | Supply Chain |

### Deleted Scripts
The following scripts were removed as duplicates or deprecated:
- Scripts with duplicate functionality (e.g., multiple forecast generation versions)
- Fix scripts that were one-time patches
- Test scripts that are no longer needed

## Best Practices

1. **Always run environment setup first**: `00_environment_setup.py`
2. **Check data dependencies**: Some scripts require outputs from previous scripts
3. **Use the Jupyter notebook**: For interactive exploration, use `orders-synthesis.ipynb`
4. **Monitor progress**: Scripts include progress bars and status messages
5. **Check logs**: Most scripts create detailed output for debugging

## Troubleshooting

### Common Issues

1. **Missing dependencies**
   - Run: `pip install -r requirements.txt`
   - Ensure you're using the correct conda environment

2. **Database not found**
   - Run `01_validate_databases.py` to check database status
   - Ensure previous scripts completed successfully

3. **Import errors**
   - Check Python path: scripts assume they're run from the scripts directory
   - Verify all required packages are installed

4. **Data inconsistencies**
   - Run validation scripts (33, 01) to check data integrity
   - Review error messages for specific issues

## Future Enhancements

- Parallel execution support for independent scripts
- Configuration file for pipeline parameters
- Automated testing suite
- Docker containerization for environment consistency