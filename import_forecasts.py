"""
Import forecast data from Baringa.xlsx and Aurora.xlsx into the forecasts table.

Baringa format: Year, Month, Day, Period, Reference Case
Aurora format: Datetime, Price

Both will be imported into a forecasts table with a 'source' column to distinguish them.
"""
import sqlite3
from datetime import datetime
from pathlib import Path

import pandas as pd

DATA_DIR = Path("data")
DB_PATH = DATA_DIR / "data.db"


def init_forecasts_table():
    """Create the forecasts table if it doesn't exist."""
    DATA_DIR.mkdir(exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS forecasts (
            datetime TEXT,
            year INTEGER,
            month INTEGER,
            day INTEGER,
            hour INTEGER,
            minute INTEGER,
            price_eur_per_mwh REAL,
            source TEXT,
            PRIMARY KEY (datetime, source)
        )
        """
    )
    conn.commit()
    conn.close()
    print("Forecasts table initialized.")


def import_baringa():
    """Import Baringa forecast data."""
    print("\nImporting Baringa forecasts...")
    df = pd.read_excel('forecasts/baringa.xlsx', sheet_name='Sheet1')
    
    # Period appears to be hour of day (0-23)
    # Convert Period to hour and set minute to 0
    df['hour'] = df['Period']
    df['minute'] = 0
    
    # Create datetime string in format matching historical_prices table
    df['datetime'] = pd.to_datetime(
        df[['Year', 'Month', 'Day', 'hour']]
    ).dt.strftime('%Y-%m-%d %H:%M:%S')
    
    # Rename Reference Case to price_eur_per_mwh
    df = df.rename(columns={'Reference Case': 'price_eur_per_mwh'})
    
    # Select and reorder columns
    df_out = df[[
        'datetime', 'Year', 'Month', 'Day', 'hour', 'minute', 
        'price_eur_per_mwh'
    ]].copy()
    df_out = df_out.rename(columns={
        'Year': 'year',
        'Month': 'month',
        'Day': 'day',
    })
    
    # Add source column
    df_out['source'] = 'Baringa_Q2_2025'
    
    # Convert to string for datetime
    df_out['datetime'] = df_out['datetime'].astype(str)
    
    # Insert into database
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    
    rows = list(df_out.itertuples(index=False, name=None))
    cur.executemany(
        """
        INSERT OR REPLACE INTO forecasts
        (datetime, year, month, day, hour, minute, price_eur_per_mwh, source)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    conn.commit()
    conn.close()
    
    print(f"Imported {len(df_out)} rows from Baringa.")


def import_aurora():
    """Import Aurora forecast data."""
    print("\nImporting Aurora forecasts...")
    df = pd.read_excel('forecasts/aurora.xlsx', sheet_name=0)
    
    # Parse datetime column
    df['datetime_parsed'] = pd.to_datetime(df['Datetime'], errors='coerce')
    df = df.dropna(subset=['datetime_parsed'])
    
    # Extract date components
    df['year'] = df['datetime_parsed'].dt.year
    df['month'] = df['datetime_parsed'].dt.month
    df['day'] = df['datetime_parsed'].dt.day
    df['hour'] = df['datetime_parsed'].dt.hour
    df['minute'] = df['datetime_parsed'].dt.minute
    
    # Create datetime string in format matching historical_prices table
    df['datetime'] = df['datetime_parsed'].dt.strftime('%Y-%m-%d %H:%M:%S')
    
    # Rename Price to price_eur_per_mwh
    df = df.rename(columns={'Price': 'price_eur_per_mwh'})
    
    # Select and reorder columns
    df_out = df[[
        'datetime', 'year', 'month', 'day', 'hour', 'minute', 
        'price_eur_per_mwh'
    ]].copy()
    
    # Add source column
    df_out['source'] = 'Aurora_Jun_2025'
    
    # Convert to string for datetime
    df_out['datetime'] = df_out['datetime'].astype(str)
    
    # Insert into database
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    
    rows = list(df_out.itertuples(index=False, name=None))
    cur.executemany(
        """
        INSERT OR REPLACE INTO forecasts
        (datetime, year, month, day, hour, minute, price_eur_per_mwh, source)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    conn.commit()
    conn.close()
    
    print(f"Imported {len(df_out)} rows from Aurora.")


def main():
    """Main import function."""
    print("Starting forecast import...")
    init_forecasts_table()
    
    try:
        import_baringa()
    except Exception as e:
        print(f"Error importing Baringa: {e}")
    
    try:
        import_aurora()
    except Exception as e:
        print(f"Error importing Aurora: {e}")
    
    # Show summary
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT source, COUNT(*) FROM forecasts GROUP BY source")
    print("\nImport summary:")
    for row in cur.fetchall():
        print(f"  {row[0]}: {row[1]} rows")
    conn.close()
    
    print("\nForecast import complete!")


if __name__ == "__main__":
    main()

