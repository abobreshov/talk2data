#!/usr/bin/env python3
"""
Analyze forecast accuracy by comparing predictions to actual sales.
Generates accuracy metrics and visualizations.
"""

import duckdb
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import seaborn as sns
from colorama import init, Fore, Style

# Initialize colorama
init()

DATA_DIR = Path(__file__).parent.parent / 'data'
DB_PATH = DATA_DIR / 'grocery_final.db'

def print_header(text):
    """Print formatted header"""
    print(f"\n{Fore.CYAN}{'='*60}{Style.RESET_ALL}")
    print(f"{Fore.CYAN}{text}{Style.RESET_ALL}")
    print(f"{Fore.CYAN}{'='*60}{Style.RESET_ALL}")

def analyze_forecast_accuracy():
    """Analyze forecast accuracy metrics"""
    
    print_header("Forecast Accuracy Analysis")
    print(f"Timestamp: {datetime.now()}")
    
    # Connect to database
    conn = duckdb.connect(str(DB_PATH))
    
    # 1. Get forecast vs actual data
    print("\n1. Loading forecast and actual sales data...")
    
    accuracy_data = conn.execute("""
        WITH forecast_actuals AS (
            SELECT 
                f.productId,
                p.name as product_name,
                p.category,
                f.target_date,
                f.predicted_quantity,
                f.confidence_lower,
                f.confidence_upper,
                COALESCE(s.actual_quantity, 0) as actual_quantity
            FROM forecasts f
            JOIN products p ON f.productId = p.productId
            LEFT JOIN (
                SELECT 
                    productId,
                    DATE_TRUNC('day', saleDate) as sale_date,
                    SUM(quantity) as actual_quantity
                FROM sales
                GROUP BY productId, DATE_TRUNC('day', saleDate)
            ) s ON f.productId = s.productId AND f.target_date = s.sale_date
            WHERE f.target_date <= CURRENT_DATE
        )
        SELECT 
            *,
            ABS(predicted_quantity - actual_quantity) as absolute_error,
            CASE 
                WHEN actual_quantity > 0 
                THEN ABS(predicted_quantity - actual_quantity) / actual_quantity 
                ELSE NULL 
            END as relative_error,
            CASE 
                WHEN actual_quantity >= confidence_lower 
                AND actual_quantity <= confidence_upper 
                THEN 1 ELSE 0 
            END as within_confidence
        FROM forecast_actuals
    """).fetchdf()
    
    print(f"  Found {len(accuracy_data):,} forecast-actual pairs")
    
    # 2. Calculate overall metrics
    print("\n2. Overall Accuracy Metrics:")
    
    # Mean Absolute Error (MAE)
    mae = accuracy_data['absolute_error'].mean()
    print(f"  - Mean Absolute Error (MAE): {mae:.2f} units")
    
    # Mean Absolute Percentage Error (MAPE)
    mape_data = accuracy_data[accuracy_data['actual_quantity'] > 0]
    if not mape_data.empty:
        mape = (mape_data['relative_error'] * 100).mean()
        print(f"  - Mean Absolute Percentage Error (MAPE): {mape:.1f}%")
    
    # Root Mean Square Error (RMSE)
    rmse = np.sqrt(((accuracy_data['predicted_quantity'] - accuracy_data['actual_quantity']) ** 2).mean())
    print(f"  - Root Mean Square Error (RMSE): {rmse:.2f} units")
    
    # Confidence interval accuracy
    conf_accuracy = accuracy_data['within_confidence'].mean() * 100
    print(f"  - Predictions within confidence interval: {conf_accuracy:.1f}%")
    
    # 3. Accuracy by category
    print("\n3. Accuracy by Product Category:")
    category_metrics = accuracy_data.groupby('category').agg({
        'absolute_error': 'mean',
        'relative_error': 'mean',
        'within_confidence': 'mean'
    }).round(2)
    
    for category, row in category_metrics.iterrows():
        mae_cat = row['absolute_error']
        mape_cat = row['relative_error'] * 100 if not pd.isna(row['relative_error']) else 0
        conf_cat = row['within_confidence'] * 100
        print(f"  - {category:<20}: MAE={mae_cat:.1f}, MAPE={mape_cat:.1f}%, Conf={conf_cat:.1f}%")
    
    # 4. Top overestimated products
    print("\n4. Top 5 Overestimated Products:")
    overestimated = accuracy_data.nlargest(5, 'absolute_error')[
        accuracy_data['predicted_quantity'] > accuracy_data['actual_quantity']
    ]
    
    for _, row in overestimated.iterrows():
        error = row['predicted_quantity'] - row['actual_quantity']
        print(f"  - {row['product_name'][:30]:<30}: Predicted={row['predicted_quantity']:.0f}, "
              f"Actual={row['actual_quantity']:.0f}, Error=+{error:.0f}")
    
    # 5. Top underestimated products
    print("\n5. Top 5 Underestimated Products:")
    underestimated = accuracy_data.nlargest(5, 'absolute_error')[
        accuracy_data['predicted_quantity'] < accuracy_data['actual_quantity']
    ]
    
    for _, row in underestimated.iterrows():
        error = row['actual_quantity'] - row['predicted_quantity']
        print(f"  - {row['product_name'][:30]:<30}: Predicted={row['predicted_quantity']:.0f}, "
              f"Actual={row['actual_quantity']:.0f}, Error=-{error:.0f}")
    
    # 6. Create visualizations
    print("\n6. Creating visualizations...")
    
    # Set up the plot style
    plt.style.use('seaborn-v0_8-darkgrid')
    fig, axes = plt.subplots(2, 2, figsize=(15, 10))
    fig.suptitle('Forecast Accuracy Analysis', fontsize=16)
    
    # Plot 1: Predicted vs Actual scatter
    ax1 = axes[0, 0]
    sample_data = accuracy_data.sample(min(1000, len(accuracy_data)))
    ax1.scatter(sample_data['actual_quantity'], sample_data['predicted_quantity'], 
                alpha=0.5, s=30)
    max_val = max(sample_data['actual_quantity'].max(), sample_data['predicted_quantity'].max())
    ax1.plot([0, max_val], [0, max_val], 'r--', lw=2, label='Perfect prediction')
    ax1.set_xlabel('Actual Quantity')
    ax1.set_ylabel('Predicted Quantity')
    ax1.set_title('Predicted vs Actual Sales')
    ax1.legend()
    
    # Plot 2: Error distribution
    ax2 = axes[0, 1]
    errors = accuracy_data['predicted_quantity'] - accuracy_data['actual_quantity']
    ax2.hist(errors, bins=50, edgecolor='black', alpha=0.7)
    ax2.axvline(x=0, color='red', linestyle='--', linewidth=2)
    ax2.set_xlabel('Prediction Error (Predicted - Actual)')
    ax2.set_ylabel('Frequency')
    ax2.set_title('Error Distribution')
    
    # Plot 3: MAE by category
    ax3 = axes[1, 0]
    category_mae = accuracy_data.groupby('category')['absolute_error'].mean().sort_values()
    category_mae.plot(kind='barh', ax=ax3)
    ax3.set_xlabel('Mean Absolute Error')
    ax3.set_title('MAE by Product Category')
    
    # Plot 4: Accuracy over time
    ax4 = axes[1, 1]
    time_accuracy = accuracy_data.groupby('target_date').agg({
        'absolute_error': 'mean',
        'within_confidence': 'mean'
    })
    ax4.plot(time_accuracy.index, time_accuracy['absolute_error'], label='MAE', marker='o')
    ax4_twin = ax4.twinx()
    ax4_twin.plot(time_accuracy.index, time_accuracy['within_confidence'] * 100, 
                  label='Conf %', color='green', marker='s')
    ax4.set_xlabel('Date')
    ax4.set_ylabel('MAE')
    ax4_twin.set_ylabel('Within Confidence %')
    ax4.set_title('Accuracy Over Time')
    ax4.legend(loc='upper left')
    ax4_twin.legend(loc='upper right')
    
    plt.tight_layout()
    
    # Save plot
    plot_path = DATA_DIR / 'forecast_accuracy_analysis.png'
    plt.savefig(plot_path, dpi=300, bbox_inches='tight')
    print(f"  {Fore.GREEN}✓{Style.RESET_ALL} Saved visualization to {plot_path}")
    
    # 7. Save detailed report
    print("\n7. Saving detailed report...")
    
    report = {
        'generated_at': datetime.now().isoformat(),
        'overall_metrics': {
            'mae': float(mae),
            'mape': float(mape) if 'mape' in locals() else None,
            'rmse': float(rmse),
            'confidence_accuracy': float(conf_accuracy)
        },
        'category_metrics': category_metrics.to_dict('index'),
        'total_predictions': len(accuracy_data),
        'date_range': {
            'start': str(accuracy_data['target_date'].min()),
            'end': str(accuracy_data['target_date'].max())
        }
    }
    
    import json
    report_path = DATA_DIR / 'forecast_accuracy_report.json'
    with open(report_path, 'w') as f:
        json.dump(report, f, indent=2)
    print(f"  {Fore.GREEN}✓{Style.RESET_ALL} Saved report to {report_path}")
    
    conn.close()
    print(f"\n✓ Forecast accuracy analysis completed")

if __name__ == "__main__":
    analyze_forecast_accuracy()