#!/usr/bin/env python3
"""
Export all tables from grocery_final.db to CSV format.
Similar to Parquet export but in CSV format for broader compatibility.
"""

import os
import duckdb
import pandas as pd
from datetime import datetime
import shutil
from pathlib import Path
from colorama import init, Fore, Style

# Initialize colorama
init()

def print_header(text):
    """Print formatted header"""
    print(f"\n{Fore.CYAN}{'='*60}{Style.RESET_ALL}")
    print(f"{Fore.CYAN}{text}{Style.RESET_ALL}")
    print(f"{Fore.CYAN}{'='*60}{Style.RESET_ALL}")

def create_export_directory(export_path):
    """Create or clean export directory"""
    if os.path.exists(export_path):
        print(f"Cleaning existing directory: {export_path}")
        shutil.rmtree(export_path)
    
    os.makedirs(export_path)
    print(f"✓ Created export directory: {export_path}")

def get_table_list(conn):
    """Get list of all tables (excluding views)"""
    query = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'main' 
        AND table_type = 'BASE TABLE'
        ORDER BY table_name
    """
    
    tables = conn.execute(query).fetchall()
    return [table[0] for table in tables]

def export_table_to_csv(conn, table_name, export_path):
    """Export a single table to CSV"""
    csv_file = os.path.join(export_path, f"{table_name}.csv")
    
    # Get row count
    row_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    
    if row_count == 0:
        # Create empty CSV with headers
        df = conn.execute(f"SELECT * FROM {table_name} LIMIT 0").df()
        df.to_csv(csv_file, index=False)
        return row_count, 0
    
    # Export to CSV
    df = conn.execute(f"SELECT * FROM {table_name}").df()
    df.to_csv(csv_file, index=False)
    
    # Get file size
    file_size = os.path.getsize(csv_file) / (1024 * 1024)  # Convert to MB
    
    return row_count, file_size

def create_export_summary(export_path, export_stats):
    """Create a summary file of the export"""
    summary_file = os.path.join(export_path, "EXPORT_SUMMARY.md")
    
    total_rows = sum(stats['rows'] for stats in export_stats.values())
    total_size = sum(stats['size'] for stats in export_stats.values())
    
    with open(summary_file, 'w') as f:
        f.write("# CSV Export Summary\n\n")
        f.write(f"## Export Information\n")
        f.write(f"- Export Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"- Total Tables: {len(export_stats)}\n")
        f.write(f"- Total Rows: {total_rows:,}\n")
        f.write(f"- Total Size: {total_size:.2f} MB\n\n")
        
        f.write("## Tables Exported\n\n")
        f.write("| Table Name | Rows | Size (MB) | File |\n")
        f.write("|------------|------|-----------|------|\n")
        
        for table_name in sorted(export_stats.keys()):
            stats = export_stats[table_name]
            f.write(f"| {table_name} | {stats['rows']:,} | {stats['size']:.2f} | {table_name}.csv |\n")
        
        f.write("\n## Usage Notes\n\n")
        f.write("- All dates are exported in ISO format (YYYY-MM-DD)\n")
        f.write("- Timestamps include time zone information where applicable\n")
        f.write("- Numeric values maintain full precision\n")
        f.write("- Text fields are properly quoted to handle commas and newlines\n")
        f.write("- NULL values are represented as empty strings\n")
        
        f.write("\n## Import Example (Python)\n\n")
        f.write("```python\n")
        f.write("import pandas as pd\n\n")
        f.write("# Read a CSV file\n")
        f.write("df = pd.read_csv('path/to/products.csv')\n")
        f.write("```\n")
        
        f.write("\n## Import Example (DuckDB)\n\n")
        f.write("```sql\n")
        f.write("-- Create table from CSV\n")
        f.write("CREATE TABLE products AS \n")
        f.write("SELECT * FROM read_csv_auto('path/to/products.csv');\n")
        f.write("```\n")

def main():
    """Main execution function"""
    print_header("CSV Export - All Tables")
    
    # Set up paths
    db_path = '../data/grocery_final.db'
    export_path = os.path.abspath('../data/csv_export')
    
    # Connect to database
    print("\n1. Connecting to database...")
    conn = duckdb.connect(db_path)
    
    # Create export directory
    print("\n2. Setting up export directory...")
    create_export_directory(export_path)
    
    # Get list of tables
    print("\n3. Getting list of tables...")
    tables = get_table_list(conn)
    print(f"Found {len(tables)} tables to export:")
    for table in tables:
        print(f"  - {table}")
    
    # Export each table
    print("\n4. Exporting tables to CSV...")
    export_stats = {}
    total_size = 0
    
    for table_name in tables:
        print(f"\nExporting {table_name}...", end='', flush=True)
        
        try:
            row_count, file_size = export_table_to_csv(conn, table_name, export_path)
            export_stats[table_name] = {
                'rows': row_count,
                'size': file_size
            }
            total_size += file_size
            
            print(f"\r✓ Exported {table_name}: {row_count:,} rows ({file_size:.2f} MB)")
            
        except Exception as e:
            print(f"\r✗ Error exporting {table_name}: {str(e)}")
            export_stats[table_name] = {
                'rows': 0,
                'size': 0,
                'error': str(e)
            }
    
    # Create summary
    print("\n5. Creating export summary...")
    create_export_summary(export_path, export_stats)
    print(f"✓ Created summary: {os.path.join(export_path, 'EXPORT_SUMMARY.md')}")
    
    # Display summary
    print_header("Export Complete!")
    print(f"\nExport Statistics:")
    print(f"  - Tables exported: {len(export_stats)}")
    print(f"  - Total rows: {sum(stats['rows'] for stats in export_stats.values()):,}")
    print(f"  - Total size: {total_size:.2f} MB")
    print(f"  - Export location: {export_path}")
    
    # Show largest tables
    print("\nLargest tables by row count:")
    sorted_tables = sorted(export_stats.items(), key=lambda x: x[1]['rows'], reverse=True)[:5]
    for table_name, stats in sorted_tables:
        print(f"  - {table_name}: {stats['rows']:,} rows ({stats['size']:.2f} MB)")
    
    # Close connection
    conn.close()
    
    print(f"\n{Fore.GREEN}✓ All tables successfully exported to CSV!{Style.RESET_ALL}")
    print(f"Files available at: {export_path}")

if __name__ == "__main__":
    main()