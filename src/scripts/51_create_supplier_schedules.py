#!/usr/bin/env python3
"""
Generate supplier delivery schedules with PO cutoff dates for UK 2025.

Distribution:
- 10% of suppliers: 1 day lead time (order by day before delivery)
- 23% of suppliers: 2 days lead time
- 33% of suppliers: 3 days lead time
- 23% of suppliers: 4 days lead time
- 11% of suppliers: 5 days lead time

PO cannot be placed on weekends or UK bank holidays.
"""

import duckdb
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import json
from colorama import init, Fore, Style

init(autoreset=True)

# UK Bank Holidays 2025
UK_HOLIDAYS_2025 = [
    datetime(2025, 1, 1),   # New Year's Day
    datetime(2025, 4, 18),  # Good Friday
    datetime(2025, 4, 21),  # Easter Monday
    datetime(2025, 5, 5),   # Early May bank holiday
    datetime(2025, 5, 26),  # Spring bank holiday
    datetime(2025, 8, 25),  # Summer bank holiday
    datetime(2025, 12, 25), # Christmas Day
    datetime(2025, 12, 26), # Boxing Day
]

def is_business_day(date):
    """Check if a date is a business day (not weekend or holiday)."""
    if date.weekday() >= 5:  # Saturday = 5, Sunday = 6
        return False
    return date not in UK_HOLIDAYS_2025

def calculate_po_cutoff(delivery_date, lead_time_days):
    """Calculate PO cutoff date, skipping weekends and holidays."""
    cutoff_date = delivery_date
    days_counted = 0
    
    while days_counted < lead_time_days:
        cutoff_date -= timedelta(days=1)
        if is_business_day(cutoff_date):
            days_counted += 1
    
    return cutoff_date

def generate_supplier_schedules():
    """Generate supplier schedules for all suppliers."""
    print(f"\n{Fore.CYAN}{'='*60}")
    print(f"{Fore.CYAN}Generating Supplier Delivery Schedules")
    print(f"{Fore.CYAN}{'='*60}\n")
    
    # Connect to database
    db_path = Path(__file__).parent.parent / "data" / "grocery_final.db"
    conn = duckdb.connect(str(db_path))
    
    # Get all suppliers
    suppliers_df = conn.execute("SELECT supplier_id, supplier_name FROM suppliers").df()
    print(f"{Fore.GREEN}✓ Found {len(suppliers_df)} suppliers")
    
    # Define lead time distribution
    lead_time_distribution = {
        1: 0.10,  # 10% - 1 day lead time
        2: 0.23,  # 23% - 2 days lead time
        3: 0.33,  # 33% - 3 days lead time
        4: 0.23,  # 23% - 4 days lead time
        5: 0.11   # 11% - 5 days lead time
    }
    
    # Assign lead times to suppliers
    np.random.seed(42)  # For reproducibility
    lead_times = np.random.choice(
        list(lead_time_distribution.keys()),
        size=len(suppliers_df),
        p=list(lead_time_distribution.values())
    )
    
    suppliers_df['lead_time_days'] = lead_times
    
    # Generate delivery dates for 2025 (all business days)
    start_date = datetime(2025, 1, 1)
    end_date = datetime(2025, 12, 31)
    
    all_dates = []
    current_date = start_date
    while current_date <= end_date:
        if is_business_day(current_date):
            all_dates.append(current_date)
        current_date += timedelta(days=1)
    
    print(f"{Fore.GREEN}✓ Generated {len(all_dates)} business days for 2025")
    
    # Create schedule records
    schedule_records = []
    
    for _, supplier in suppliers_df.iterrows():
        supplier_id = supplier['supplier_id']
        lead_time = supplier['lead_time_days']
        
        # For each supplier, create schedules for all delivery dates
        for delivery_date in all_dates:
            po_cutoff_date = calculate_po_cutoff(delivery_date, lead_time)
            
            schedule_records.append({
                'supplier_id': supplier_id,
                'delivery_date': delivery_date.strftime('%Y-%m-%d'),
                'po_cutoff_date': po_cutoff_date.strftime('%Y-%m-%d'),
                'po_cutoff_time': '17:00:00',  # 5 PM cutoff
                'lead_time_days': lead_time
            })
    
    schedules_df = pd.DataFrame(schedule_records)
    
    # Print distribution summary
    print(f"\n{Fore.YELLOW}Lead Time Distribution:")
    distribution_summary = suppliers_df['lead_time_days'].value_counts().sort_index()
    for days, count in distribution_summary.items():
        percentage = (count / len(suppliers_df)) * 100
        print(f"  {days} day(s): {count} suppliers ({percentage:.1f}%)")
    
    # Create table and insert data
    print(f"\n{Fore.CYAN}Creating supplier_schedules table...")
    
    # Drop table if exists
    conn.execute("DROP TABLE IF EXISTS supplier_schedules")
    conn.execute("DROP SEQUENCE IF EXISTS supplier_schedules_seq")
    
    # Create table
    conn.execute("""
        CREATE SEQUENCE supplier_schedules_seq;
        CREATE TABLE supplier_schedules (
            schedule_id INTEGER PRIMARY KEY DEFAULT nextval('supplier_schedules_seq'),
            supplier_id INTEGER NOT NULL,
            delivery_date DATE NOT NULL,
            po_cutoff_date DATE NOT NULL,
            po_cutoff_time TIME NOT NULL,
            lead_time_days INTEGER NOT NULL,
            FOREIGN KEY (supplier_id) REFERENCES suppliers(supplier_id)
        )
    """)
    
    # Insert data
    conn.execute("""
        INSERT INTO supplier_schedules (supplier_id, delivery_date, po_cutoff_date, po_cutoff_time, lead_time_days)
        SELECT * FROM schedules_df
    """)
    
    # Create indexes for performance
    conn.execute("CREATE INDEX idx_supplier_schedules_supplier ON supplier_schedules(supplier_id)")
    conn.execute("CREATE INDEX idx_supplier_schedules_delivery ON supplier_schedules(delivery_date)")
    conn.execute("CREATE INDEX idx_supplier_schedules_cutoff ON supplier_schedules(po_cutoff_date)")
    
    # Verify the data
    total_schedules = conn.execute("SELECT COUNT(*) FROM supplier_schedules").fetchone()[0]
    print(f"{Fore.GREEN}✓ Created {total_schedules:,} schedule records")
    
    # Show sample schedules
    print(f"\n{Fore.YELLOW}Sample Schedules:")
    sample_schedules = conn.execute("""
        SELECT 
            s.supplier_name,
            ss.delivery_date,
            ss.po_cutoff_date,
            ss.po_cutoff_time,
            ss.lead_time_days
        FROM supplier_schedules ss
        JOIN suppliers s ON ss.supplier_id = s.supplier_id
        WHERE ss.delivery_date >= '2025-07-14' 
        AND ss.delivery_date <= '2025-07-18'
        AND s.supplier_id IN (
            SELECT supplier_id FROM suppliers ORDER BY RANDOM() LIMIT 5
        )
        ORDER BY s.supplier_name, ss.delivery_date
        LIMIT 25
    """).df()
    
    print(sample_schedules.to_string(index=False))
    
    # Create a useful view
    conn.execute("""
        CREATE OR REPLACE VIEW supplier_delivery_calendar AS
        SELECT 
            s.supplier_id,
            s.supplier_name,
            s.contact_email,
            ss.delivery_date,
            ss.po_cutoff_date,
            ss.po_cutoff_time,
            ss.lead_time_days,
            CASE 
                WHEN EXTRACT(DOW FROM ss.delivery_date) = 1 THEN 'Monday'
                WHEN EXTRACT(DOW FROM ss.delivery_date) = 2 THEN 'Tuesday'
                WHEN EXTRACT(DOW FROM ss.delivery_date) = 3 THEN 'Wednesday'
                WHEN EXTRACT(DOW FROM ss.delivery_date) = 4 THEN 'Thursday'
                WHEN EXTRACT(DOW FROM ss.delivery_date) = 5 THEN 'Friday'
            END as delivery_day_of_week
        FROM supplier_schedules ss
        JOIN suppliers s ON ss.supplier_id = s.supplier_id
    """)
    
    print(f"\n{Fore.GREEN}✓ Created view: supplier_delivery_calendar")
    
    # Save summary
    summary = {
        'total_suppliers': len(suppliers_df),
        'total_schedules': total_schedules,
        'business_days_2025': len(all_dates),
        'lead_time_distribution': {
            f"{k}_days": {
                'count': int(distribution_summary.get(k, 0)),
                'percentage': float((distribution_summary.get(k, 0) / len(suppliers_df)) * 100)
            }
            for k in sorted(lead_time_distribution.keys())
        },
        'uk_holidays_2025': [d.strftime('%Y-%m-%d') for d in UK_HOLIDAYS_2025]
    }
    
    summary_path = Path(__file__).parent.parent / 'data' / 'supplier_schedules_summary.json'
    with open(summary_path, 'w') as f:
        json.dump(summary, f, indent=2)
    
    print(f"\n{Fore.GREEN}✓ Summary saved to supplier_schedules_summary.json")
    
    conn.close()
    print(f"\n{Fore.CYAN}{'='*60}")
    print(f"{Fore.GREEN}✓ Supplier schedules generation complete!")
    print(f"{Fore.CYAN}{'='*60}\n")

if __name__ == "__main__":
    generate_supplier_schedules()