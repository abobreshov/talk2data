# Task 11: Environment Setup and Data Validation

## Objective
Set up the Python environment and validate all required data sources before starting the synthesis process.

## Steps

### 1. Verify Python Environment
- Ensure grocery_poc conda environment is activated
- Install any missing dependencies (if needed)

### 2. Validate Data Sources
- **Products Database:**
  - Location: `src/data/products.duckdb`
  - Verify FOOD category products exist
  - Check price format (pennies)
  - Count available products

- **Customers Data:**
  - Location: `customers.csv` (need to confirm location)
  - Validate CSV structure
  - Count total customers
  - Check for duplicates

- **M5 Dataset:**
  - Location: `src/notebooks/data/m5/`
  - Verify files exist
  - Understand M5 data structure
  - Identify relevant columns for order mapping

### 3. Create Working Directory
```bash
mkdir -p src/notebooks/orders-synthesis
```

### 4. Initialize DuckDB Database
```python
import duckdb

# Create orders database
conn = duckdb.connect('src/data/orders.duckdb')

# Create schema
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

conn.execute("""
    CREATE TABLE IF NOT EXISTS order_items (
        orderId VARCHAR,
        productId VARCHAR,
        quantity INTEGER,
        price DECIMAL(10,2)
    )
""")
```

## Validation Queries
```python
# Check products
products_count = conn.execute("""
    SELECT COUNT(*) 
    FROM products 
    WHERE category = 'FOOD'
""").fetchone()[0]

# Check price range
price_stats = conn.execute("""
    SELECT 
        MIN(price/100.0) as min_price,
        MAX(price/100.0) as max_price,
        AVG(price/100.0) as avg_price
    FROM products 
    WHERE category = 'FOOD'
""").fetchone()
```

## Output
- Environment validation report
- Data source availability confirmation
- Initial statistics for planning