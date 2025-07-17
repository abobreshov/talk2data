-- Databricks Script to Register Delta Tables
-- Generated: 2025-07-16 13:46:39
-- Delta tables location: /home/abobreshov/work/dataart/talk2data/grocery_poc/src/data/delta_export

-- Step 1: Create catalog and schema
CREATE CATALOG IF NOT EXISTS `grocery_poc`;
USE CATALOG `grocery_poc`;
CREATE SCHEMA IF NOT EXISTS `main`;
USE SCHEMA `main`;

-- Step 2: Register Delta tables as external tables
-- Note: Update the path to match your DBFS or external storage location

-- Step 3: Refresh table metadata

-- Step 4: Analyze tables for statistics

-- Step 5: Verify table counts
WITH table_counts AS (
)
SELECT * FROM table_counts ORDER BY table_name;
