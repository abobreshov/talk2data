#!/bin/bash
# Upload DuckDB to Databricks using CLI

echo "Uploading DuckDB file to Databricks..."

# Check if databricks CLI is installed
if ! command -v databricks &> /dev/null; then
    echo "Error: Databricks CLI not found. Please install it first:"
    echo "  pip install databricks-cli"
    exit 1
fi

# Set variables
DB_FILE="grocery_final.db"
DBFS_PATH="dbfs:/FileStore/grocery_poc/"

# Check if file exists
if [ ! -f "$DB_FILE" ]; then
    echo "Error: $DB_FILE not found in current directory"
    exit 1
fi

# Create directory in DBFS
echo "Creating directory in DBFS..."
databricks fs mkdirs $DBFS_PATH

# Upload file
echo "Uploading $DB_FILE to $DBFS_PATH..."
databricks fs cp $DB_FILE $DBFS_PATH --overwrite

# Verify upload
echo "Verifying upload..."
databricks fs ls $DBFS_PATH

echo "Upload complete!"
echo "Next steps:"
echo "1. Import direct_duckdb_upload_notebook.py to Databricks"
echo "2. Run the notebook to create Unity Catalog tables"
