let's have a delta table products. let's create it from ./docs/products/data-example-products.csv
let's assume that sku in products is productId. Let's create a table that maps productIds to skus: one productId can have several Sku's let's generate several records each productId should have random 1-3 skus.
let's use duckdb to store data.
Let's have script in notebook with generation action description. 
that would be product - skus data synthesis

## Implementation Plan

1. **Analyze products CSV** - Read the example products data from `./docs/products/data-example-products.csv` to understand structure
2. **Update existing notebook** (`/src/notebooks/products-skus-synthesis.ipynb`) with:
   - DuckDB connection setup
   - Import products from CSV into DuckDB table (treating SKU column as productId)
   - Generate product-SKU mapping table where each productId has 1-3 randomly generated SKUs
   - Document the synthesis process with markdown cells
3. **Create output tables**:
   - `products` table - imported from CSV
   - `product_skus` table - mapping of productId to generated SKUs
4. **Data synthesis approach**:
   - Use the existing SKU column as productId
   - Generate new SKU format: `{productId}{4-digit-suffix}` where suffix is 0001, 0002, 0003
   - Randomly assign 1-3 SKUs per product
