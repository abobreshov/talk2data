-- Databricks Unity Catalog Upload Script
-- Generated: 2025-07-16 13:39:06
-- Source: DuckDB grocery_final.db

-- Step 1: Create catalog and schema
CREATE CATALOG IF NOT EXISTS `grocery_poc`;
USE CATALOG `grocery_poc`;
CREATE SCHEMA IF NOT EXISTS `main`;
USE SCHEMA `main`;

-- Step 2: Create tables (in dependency order)

-- Table: customers
CREATE TABLE IF NOT EXISTS `grocery_poc`.`main`.`customers` (
  `customerId` INT NOT NULL,
  `first_name` STRING NOT NULL,
  `last_name` STRING NOT NULL,
  `email` STRING NOT NULL,
  `gender` STRING,
  `address` STRING,
  `city` STRING,
  `postcode` STRING,
  `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
USING DELTA
TBLPROPERTIES (
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2',
  'description' = 'Imported from DuckDB grocery_final.db on 2025-07-16'
);


-- Table: products
CREATE TABLE IF NOT EXISTS `grocery_poc`.`main`.`products` (
  `productId` STRING NOT NULL,
  `name` STRING NOT NULL,
  `brandName` STRING,
  `sellingSize` STRING,
  `currency` STRING DEFAULT 'GBP',
  `price_pence` INT NOT NULL,
  `category` STRING,
  `subcategory` STRING,
  `price_gbp` DECIMAL(10,2) DEFAULT CAST((price_pence / 100.0) AS DECIMAL(10,2)),
  `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  `supplier_id` INT,
  CONSTRAINT pk_products PRIMARY KEY (`productId`)
)
USING DELTA
TBLPROPERTIES (
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2',
  'description' = 'Imported from DuckDB grocery_final.db on 2025-07-16'
);


-- Table: suppliers
CREATE TABLE IF NOT EXISTS `grocery_poc`.`main`.`suppliers` (
  `supplier_id` INT NOT NULL,
  `supplier_name` STRING NOT NULL,
  `contact_email` STRING,
  `contact_phone` STRING,
  `address` STRING,
  `city` STRING,
  `postcode` STRING,
  `country` STRING DEFAULT 'UK',
  `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
USING DELTA
TBLPROPERTIES (
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2',
  'description' = 'Imported from DuckDB grocery_final.db on 2025-07-16'
);


-- Table: product_skus
CREATE TABLE IF NOT EXISTS `grocery_poc`.`main`.`product_skus` (
  `productId` STRING NOT NULL,
  `sku` STRING NOT NULL,
  `sku_suffix` STRING,
  `is_primary` BOOLEAN DEFAULT CAST('f' AS BOOLEAN),
  `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  CONSTRAINT pk_product_skus PRIMARY KEY (`productId`)
)
USING DELTA
TBLPROPERTIES (
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2',
  'description' = 'Imported from DuckDB grocery_final.db on 2025-07-16'
);


-- Table: orders
CREATE TABLE IF NOT EXISTS `grocery_poc`.`main`.`orders` (
  `orderId` STRING NOT NULL,
  `customerId` INT NOT NULL,
  `orderDate` TIMESTAMP NOT NULL,
  `deliveryDate` TIMESTAMP NOT NULL,
  `orderStatus` STRING NOT NULL,
  `totalAmount` DECIMAL(10,2) NOT NULL,
  `itemCount` INT NOT NULL,
  `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
USING DELTA
TBLPROPERTIES (
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2',
  'description' = 'Imported from DuckDB grocery_final.db on 2025-07-16'
);


-- Table: order_items
CREATE TABLE IF NOT EXISTS `grocery_poc`.`main`.`order_items` (
  `orderItemId` STRING NOT NULL,
  `orderId` STRING NOT NULL,
  `productId` STRING NOT NULL,
  `quantity` INT NOT NULL,
  `unitPrice` DECIMAL(10,2) NOT NULL,
  `totalPrice` DECIMAL(10,2) NOT NULL,
  `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  CONSTRAINT pk_order_items PRIMARY KEY (`productId`)
)
USING DELTA
TBLPROPERTIES (
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2',
  'description' = 'Imported from DuckDB grocery_final.db on 2025-07-16'
);


-- Table: sales
CREATE TABLE IF NOT EXISTS `grocery_poc`.`main`.`sales` (
  `saleId` STRING NOT NULL,
  `orderId` STRING NOT NULL,
  `orderItemId` STRING NOT NULL,
  `customerId` INT NOT NULL,
  `productId` STRING NOT NULL,
  `saleDate` TIMESTAMP NOT NULL,
  `unitPrice` DECIMAL(10,2) NOT NULL,
  `quantity` INT NOT NULL,
  `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  CONSTRAINT pk_sales PRIMARY KEY (`productId`)
)
USING DELTA
TBLPROPERTIES (
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2',
  'description' = 'Imported from DuckDB grocery_final.db on 2025-07-16'
);


-- Table: stock
CREATE TABLE IF NOT EXISTS `grocery_poc`.`main`.`stock` (
  `stock_id` INT NOT NULL,
  `sku` STRING NOT NULL,
  `productId` STRING NOT NULL,
  `quantity_in_stock` INT NOT NULL,
  `expiration_date` DATE NOT NULL,
  `batch_number` STRING NOT NULL,
  `received_date` DATE NOT NULL,
  `temperature_zone` STRING NOT NULL,
  `stock_status` STRING NOT NULL,
  `last_updated` TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  `purchase_price` DECIMAL(10,2),
  `supplier_id` INT,
  CONSTRAINT pk_stock PRIMARY KEY (`stock_id`)
)
USING DELTA
TBLPROPERTIES (
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2',
  'description' = 'Imported from DuckDB grocery_final.db on 2025-07-16'
);


-- Table: forecasts
CREATE TABLE IF NOT EXISTS `grocery_poc`.`main`.`forecasts` (
  `forecast_id` STRING NOT NULL,
  `run_id` STRING NOT NULL,
  `forecast_date` DATE NOT NULL,
  `productId` STRING NOT NULL,
  `target_date` DATE NOT NULL,
  `forecast_horizon` INT NOT NULL,
  `predicted_quantity` FLOAT NOT NULL,
  `confidence_lower` FLOAT NOT NULL,
  `confidence_upper` FLOAT NOT NULL,
  `model_name` STRING DEFAULT 'TiREx',
  `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  CONSTRAINT pk_forecasts PRIMARY KEY (`forecast_id`)
)
USING DELTA
TBLPROPERTIES (
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2',
  'description' = 'Imported from DuckDB grocery_final.db on 2025-07-16'
);


-- Table: supplier_schedules
CREATE TABLE IF NOT EXISTS `grocery_poc`.`main`.`supplier_schedules` (
  `schedule_id` INT NOT NULL,
  `supplier_id` INT NOT NULL,
  `delivery_date` DATE NOT NULL,
  `po_cutoff_date` DATE NOT NULL,
  `po_cutoff_time` STRING NOT NULL,
  `lead_time_days` INT NOT NULL
)
USING DELTA
TBLPROPERTIES (
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2',
  'description' = 'Imported from DuckDB grocery_final.db on 2025-07-16'
);


-- Table: product_ordering_calendar
CREATE TABLE IF NOT EXISTS `grocery_poc`.`main`.`product_ordering_calendar` (
  `calendar_id` INT NOT NULL,
  `productId` STRING NOT NULL,
  `supplier_id` INT,
  `order_date` DATE NOT NULL,
  `atp_delivery_date` DATE NOT NULL,
  `next_atp_delivery_date` DATE,
  `purge_date` DATE,
  `contingency_days_in_advance` INT DEFAULT 2,
  `contingency_days_of_demand` INT DEFAULT 4,
  `lead_time_days` INT DEFAULT 3,
  `min_order_quantity` INT DEFAULT 1,
  `order_multiple` INT DEFAULT 1,
  `is_active` BOOLEAN DEFAULT CAST('t' AS BOOLEAN),
  `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  CONSTRAINT pk_product_ordering_calendar PRIMARY KEY (`productId`)
)
USING DELTA
TBLPROPERTIES (
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2',
  'description' = 'Imported from DuckDB grocery_final.db on 2025-07-16'
);


-- Table: purchase_orders
CREATE TABLE IF NOT EXISTS `grocery_poc`.`main`.`purchase_orders` (
  `po_id` STRING NOT NULL,
  `supplier_id` INT NOT NULL,
  `order_date` DATE NOT NULL,
  `expected_delivery_date` DATE NOT NULL,
  `po_status` STRING DEFAULT 'PENDING',
  `total_items` INT,
  `total_amount` DECIMAL(10,2),
  `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
USING DELTA
TBLPROPERTIES (
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2',
  'description' = 'Imported from DuckDB grocery_final.db on 2025-07-16'
);


-- Table: inbound_deliveries
CREATE TABLE IF NOT EXISTS `grocery_poc`.`main`.`inbound_deliveries` (
  `delivery_id` STRING NOT NULL,
  `po_id` STRING NOT NULL,
  `productId` STRING NOT NULL,
  `sku` STRING NOT NULL,
  `quantity` INT NOT NULL,
  `unit_cost` DECIMAL(10,2),
  `expected_delivery_date` DATE NOT NULL,
  `actual_delivery_date` DATE,
  `expiration_date` DATE NOT NULL,
  `batch_number` STRING,
  `status` STRING DEFAULT 'PENDING',
  `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  CONSTRAINT pk_inbound_deliveries PRIMARY KEY (`productId`)
)
USING DELTA
TBLPROPERTIES (
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2',
  'description' = 'Imported from DuckDB grocery_final.db on 2025-07-16'
);


-- Table: live_basket
CREATE TABLE IF NOT EXISTS `grocery_poc`.`main`.`live_basket` (
  `basket_id` INT NOT NULL,
  `session_id` STRING NOT NULL,
  `customerId` INT,
  `productId` STRING NOT NULL,
  `quantity` INT NOT NULL,
  `added_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  `expires_at` TIMESTAMP,
  `status` STRING DEFAULT 'ACTIVE',
  CONSTRAINT pk_live_basket PRIMARY KEY (`productId`)
)
USING DELTA
TBLPROPERTIES (
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2',
  'description' = 'Imported from DuckDB grocery_final.db on 2025-07-16'
);


-- Table: product_purge_reference
CREATE TABLE IF NOT EXISTS `grocery_poc`.`main`.`product_purge_reference` (
  `reference_id` INT NOT NULL,
  `category` STRING NOT NULL,
  `subcategory` STRING,
  `shelf_life_days` INT NOT NULL,
  `min_shelf_life_days` INT NOT NULL,
  `max_shelf_life_days` INT NOT NULL,
  `temperature_sensitive` BOOLEAN DEFAULT CAST('f' AS BOOLEAN),
  `requires_refrigeration` BOOLEAN DEFAULT CAST('f' AS BOOLEAN),
  `product_count` INT,
  `notes` STRING,
  `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
USING DELTA
TBLPROPERTIES (
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2',
  'description' = 'Imported from DuckDB grocery_final.db on 2025-07-16'
);


-- Table: purge_log
CREATE TABLE IF NOT EXISTS `grocery_poc`.`main`.`purge_log` (
  `purge_id` INT NOT NULL,
  `sku` STRING NOT NULL,
  `productId` STRING NOT NULL,
  `quantity_purged` INT NOT NULL,
  `purge_date` DATE NOT NULL,
  `purge_reason` STRING,
  `batch_number` STRING,
  `original_expiration_date` DATE,
  `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  CONSTRAINT pk_purge_log PRIMARY KEY (`sku`)
)
USING DELTA
TBLPROPERTIES (
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2',
  'description' = 'Imported from DuckDB grocery_final.db on 2025-07-16'
);

-- Step 3: Load data using COPY INTO
-- Note: Upload Parquet files to DBFS or external storage first
-- Suggested path: dbfs:/FileStore/grocery_poc/

-- Load customers
COPY INTO `grocery_poc`.`main`.`customers`
FROM 'dbfs:/FileStore/grocery_poc/customers.parquet'
FILEFORMAT = PARQUET
FORMAT_OPTIONS ('mergeSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');

-- Load products
COPY INTO `grocery_poc`.`main`.`products`
FROM 'dbfs:/FileStore/grocery_poc/products.parquet'
FILEFORMAT = PARQUET
FORMAT_OPTIONS ('mergeSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');

-- Load suppliers
COPY INTO `grocery_poc`.`main`.`suppliers`
FROM 'dbfs:/FileStore/grocery_poc/suppliers.parquet'
FILEFORMAT = PARQUET
FORMAT_OPTIONS ('mergeSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');

-- Load product_skus
COPY INTO `grocery_poc`.`main`.`product_skus`
FROM 'dbfs:/FileStore/grocery_poc/product_skus.parquet'
FILEFORMAT = PARQUET
FORMAT_OPTIONS ('mergeSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');

-- Load orders
COPY INTO `grocery_poc`.`main`.`orders`
FROM 'dbfs:/FileStore/grocery_poc/orders.parquet'
FILEFORMAT = PARQUET
FORMAT_OPTIONS ('mergeSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');

-- Load order_items
COPY INTO `grocery_poc`.`main`.`order_items`
FROM 'dbfs:/FileStore/grocery_poc/order_items.parquet'
FILEFORMAT = PARQUET
FORMAT_OPTIONS ('mergeSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');

-- Load sales
COPY INTO `grocery_poc`.`main`.`sales`
FROM 'dbfs:/FileStore/grocery_poc/sales.parquet'
FILEFORMAT = PARQUET
FORMAT_OPTIONS ('mergeSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');

-- Load stock
COPY INTO `grocery_poc`.`main`.`stock`
FROM 'dbfs:/FileStore/grocery_poc/stock.parquet'
FILEFORMAT = PARQUET
FORMAT_OPTIONS ('mergeSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');

-- Load forecasts
COPY INTO `grocery_poc`.`main`.`forecasts`
FROM 'dbfs:/FileStore/grocery_poc/forecasts.parquet'
FILEFORMAT = PARQUET
FORMAT_OPTIONS ('mergeSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');

-- Load supplier_schedules
COPY INTO `grocery_poc`.`main`.`supplier_schedules`
FROM 'dbfs:/FileStore/grocery_poc/supplier_schedules.parquet'
FILEFORMAT = PARQUET
FORMAT_OPTIONS ('mergeSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');

-- Load product_ordering_calendar
COPY INTO `grocery_poc`.`main`.`product_ordering_calendar`
FROM 'dbfs:/FileStore/grocery_poc/product_ordering_calendar.parquet'
FILEFORMAT = PARQUET
FORMAT_OPTIONS ('mergeSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');

-- Load purchase_orders
COPY INTO `grocery_poc`.`main`.`purchase_orders`
FROM 'dbfs:/FileStore/grocery_poc/purchase_orders.parquet'
FILEFORMAT = PARQUET
FORMAT_OPTIONS ('mergeSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');

-- Load inbound_deliveries
COPY INTO `grocery_poc`.`main`.`inbound_deliveries`
FROM 'dbfs:/FileStore/grocery_poc/inbound_deliveries.parquet'
FILEFORMAT = PARQUET
FORMAT_OPTIONS ('mergeSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');

-- Load live_basket
COPY INTO `grocery_poc`.`main`.`live_basket`
FROM 'dbfs:/FileStore/grocery_poc/live_basket.parquet'
FILEFORMAT = PARQUET
FORMAT_OPTIONS ('mergeSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');

-- Load product_purge_reference
COPY INTO `grocery_poc`.`main`.`product_purge_reference`
FROM 'dbfs:/FileStore/grocery_poc/product_purge_reference.parquet'
FILEFORMAT = PARQUET
FORMAT_OPTIONS ('mergeSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');

-- Load purge_log
COPY INTO `grocery_poc`.`main`.`purge_log`
FROM 'dbfs:/FileStore/grocery_poc/purge_log.parquet'
FILEFORMAT = PARQUET
FORMAT_OPTIONS ('mergeSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');

-- Step 4: Add foreign key constraints
ALTER TABLE `grocery_poc`.`main`.`product_skus`
ADD CONSTRAINT fk_product_skus_productId
FOREIGN KEY (`productId`) REFERENCES `grocery_poc`.`main`.`products`(`productId`);

ALTER TABLE `grocery_poc`.`main`.`order_items`
ADD CONSTRAINT fk_order_items_orderId
FOREIGN KEY (`orderId`) REFERENCES `grocery_poc`.`main`.`orders`(`orderId`);

ALTER TABLE `grocery_poc`.`main`.`order_items`
ADD CONSTRAINT fk_order_items_productId
FOREIGN KEY (`productId`) REFERENCES `grocery_poc`.`main`.`products`(`productId`);

ALTER TABLE `grocery_poc`.`main`.`sales`
ADD CONSTRAINT fk_sales_orderId
FOREIGN KEY (`orderId`) REFERENCES `grocery_poc`.`main`.`orders`(`orderId`);

ALTER TABLE `grocery_poc`.`main`.`sales`
ADD CONSTRAINT fk_sales_customerId
FOREIGN KEY (`customerId`) REFERENCES `grocery_poc`.`main`.`customers`(`id`);

ALTER TABLE `grocery_poc`.`main`.`sales`
ADD CONSTRAINT fk_sales_productId
FOREIGN KEY (`productId`) REFERENCES `grocery_poc`.`main`.`products`(`productId`);

ALTER TABLE `grocery_poc`.`main`.`stock`
ADD CONSTRAINT fk_stock_productId
FOREIGN KEY (`productId`) REFERENCES `grocery_poc`.`main`.`products`(`productId`);

ALTER TABLE `grocery_poc`.`main`.`stock`
ADD CONSTRAINT fk_stock_sku
FOREIGN KEY (`sku`) REFERENCES `grocery_poc`.`main`.`product_skus`(`sku`);

ALTER TABLE `grocery_poc`.`main`.`stock`
ADD CONSTRAINT fk_stock_supplier_id
FOREIGN KEY (`supplier_id`) REFERENCES `grocery_poc`.`main`.`suppliers`(`supplier_id`);

ALTER TABLE `grocery_poc`.`main`.`forecasts`
ADD CONSTRAINT fk_forecasts_productId
FOREIGN KEY (`productId`) REFERENCES `grocery_poc`.`main`.`products`(`productId`);

ALTER TABLE `grocery_poc`.`main`.`supplier_schedules`
ADD CONSTRAINT fk_supplier_schedules_supplier_id
FOREIGN KEY (`supplier_id`) REFERENCES `grocery_poc`.`main`.`suppliers`(`supplier_id`);

ALTER TABLE `grocery_poc`.`main`.`product_ordering_calendar`
ADD CONSTRAINT fk_product_ordering_calendar_productId
FOREIGN KEY (`productId`) REFERENCES `grocery_poc`.`main`.`products`(`productId`);

ALTER TABLE `grocery_poc`.`main`.`product_ordering_calendar`
ADD CONSTRAINT fk_product_ordering_calendar_supplier_id
FOREIGN KEY (`supplier_id`) REFERENCES `grocery_poc`.`main`.`suppliers`(`supplier_id`);

ALTER TABLE `grocery_poc`.`main`.`purchase_orders`
ADD CONSTRAINT fk_purchase_orders_productId
FOREIGN KEY (`productId`) REFERENCES `grocery_poc`.`main`.`products`(`productId`);

ALTER TABLE `grocery_poc`.`main`.`purchase_orders`
ADD CONSTRAINT fk_purchase_orders_supplier_id
FOREIGN KEY (`supplier_id`) REFERENCES `grocery_poc`.`main`.`suppliers`(`supplier_id`);

ALTER TABLE `grocery_poc`.`main`.`inbound_deliveries`
ADD CONSTRAINT fk_inbound_deliveries_purchase_order_id
FOREIGN KEY (`purchase_order_id`) REFERENCES `grocery_poc`.`main`.`purchase_orders`(`purchase_order_id`);

ALTER TABLE `grocery_poc`.`main`.`inbound_deliveries`
ADD CONSTRAINT fk_inbound_deliveries_productId
FOREIGN KEY (`productId`) REFERENCES `grocery_poc`.`main`.`products`(`productId`);

ALTER TABLE `grocery_poc`.`main`.`live_basket`
ADD CONSTRAINT fk_live_basket_productId
FOREIGN KEY (`productId`) REFERENCES `grocery_poc`.`main`.`products`(`productId`);

-- Step 5: Update table statistics
ANALYZE TABLE `grocery_poc`.`main`.`customers` COMPUTE STATISTICS;
ANALYZE TABLE `grocery_poc`.`main`.`products` COMPUTE STATISTICS;
ANALYZE TABLE `grocery_poc`.`main`.`suppliers` COMPUTE STATISTICS;
ANALYZE TABLE `grocery_poc`.`main`.`product_skus` COMPUTE STATISTICS;
ANALYZE TABLE `grocery_poc`.`main`.`orders` COMPUTE STATISTICS;
ANALYZE TABLE `grocery_poc`.`main`.`order_items` COMPUTE STATISTICS;
ANALYZE TABLE `grocery_poc`.`main`.`sales` COMPUTE STATISTICS;
ANALYZE TABLE `grocery_poc`.`main`.`stock` COMPUTE STATISTICS;
ANALYZE TABLE `grocery_poc`.`main`.`forecasts` COMPUTE STATISTICS;
ANALYZE TABLE `grocery_poc`.`main`.`supplier_schedules` COMPUTE STATISTICS;
ANALYZE TABLE `grocery_poc`.`main`.`product_ordering_calendar` COMPUTE STATISTICS;
ANALYZE TABLE `grocery_poc`.`main`.`purchase_orders` COMPUTE STATISTICS;
ANALYZE TABLE `grocery_poc`.`main`.`inbound_deliveries` COMPUTE STATISTICS;
ANALYZE TABLE `grocery_poc`.`main`.`live_basket` COMPUTE STATISTICS;
ANALYZE TABLE `grocery_poc`.`main`.`product_purge_reference` COMPUTE STATISTICS;
ANALYZE TABLE `grocery_poc`.`main`.`purge_log` COMPUTE STATISTICS;

-- Step 6: Verify row counts
SELECT 'customers' as table_name, COUNT(*) as row_count FROM `grocery_poc`.`main`.`customers`
UNION ALL
SELECT 'products' as table_name, COUNT(*) as row_count FROM `grocery_poc`.`main`.`products`
UNION ALL
SELECT 'suppliers' as table_name, COUNT(*) as row_count FROM `grocery_poc`.`main`.`suppliers`
UNION ALL
SELECT 'product_skus' as table_name, COUNT(*) as row_count FROM `grocery_poc`.`main`.`product_skus`
UNION ALL
SELECT 'orders' as table_name, COUNT(*) as row_count FROM `grocery_poc`.`main`.`orders`
UNION ALL
SELECT 'order_items' as table_name, COUNT(*) as row_count FROM `grocery_poc`.`main`.`order_items`
UNION ALL
SELECT 'sales' as table_name, COUNT(*) as row_count FROM `grocery_poc`.`main`.`sales`
UNION ALL
SELECT 'stock' as table_name, COUNT(*) as row_count FROM `grocery_poc`.`main`.`stock`
UNION ALL
SELECT 'forecasts' as table_name, COUNT(*) as row_count FROM `grocery_poc`.`main`.`forecasts`
UNION ALL
SELECT 'supplier_schedules' as table_name, COUNT(*) as row_count FROM `grocery_poc`.`main`.`supplier_schedules`
UNION ALL
SELECT 'product_ordering_calendar' as table_name, COUNT(*) as row_count FROM `grocery_poc`.`main`.`product_ordering_calendar`
UNION ALL
SELECT 'purchase_orders' as table_name, COUNT(*) as row_count FROM `grocery_poc`.`main`.`purchase_orders`
UNION ALL
SELECT 'inbound_deliveries' as table_name, COUNT(*) as row_count FROM `grocery_poc`.`main`.`inbound_deliveries`
UNION ALL
SELECT 'live_basket' as table_name, COUNT(*) as row_count FROM `grocery_poc`.`main`.`live_basket`
UNION ALL
SELECT 'product_purge_reference' as table_name, COUNT(*) as row_count FROM `grocery_poc`.`main`.`product_purge_reference`
UNION ALL
SELECT 'purge_log' as table_name, COUNT(*) as row_count FROM `grocery_poc`.`main`.`purge_log`
UNION ALL
SELECT 'TOTAL' as table_name, 0 as row_count;