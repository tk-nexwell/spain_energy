"""
Script to rename spot_prices table to historical_prices
and day_ahead_prices column to ESIOS_600_DA_prices
"""
import sqlite3
from pathlib import Path

from db import DB_PATH

def rename_table_and_column():
    """Rename spot_prices table to historical_prices and day_ahead_prices to ESIOS_600_DA_prices"""
    if not DB_PATH.exists():
        print(f"Database {DB_PATH} does not exist. Nothing to rename.")
        return
    
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    
    try:
        # Check if spot_prices table exists
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='spot_prices'")
        if cur.fetchone() is None:
            print("Table 'spot_prices' does not exist. Nothing to rename.")
            return
        
        # Check if day_ahead_prices column exists
        cur.execute("PRAGMA table_info(spot_prices)")
        columns = [row[1] for row in cur.fetchall()]
        if "day_ahead_prices" not in columns:
            print("Column 'day_ahead_prices' does not exist. Nothing to rename.")
            return
        
        print("Renaming table 'spot_prices' to 'historical_prices'...")
        # SQLite doesn't support direct table rename, so we need to:
        # 1. Create new table with new name and new column name
        # 2. Copy data
        # 3. Drop old table
        
        # Get the table structure
        cur.execute("PRAGMA table_info(spot_prices)")
        table_info = cur.fetchall()
        
        # Create new table with renamed column
        create_sql = "CREATE TABLE IF NOT EXISTS historical_prices ("
        columns_sql = []
        for col_info in table_info:
            col_name = col_info[1]
            col_type = col_info[2]
            is_pk = col_info[5]
            
            if col_name == "day_ahead_prices":
                col_name = "ESIOS_600_DA_prices"
            
            col_def = f"{col_name} {col_type}"
            if is_pk:
                col_def += " PRIMARY KEY"
            columns_sql.append(col_def)
        
        create_sql += ", ".join(columns_sql) + ")"
        cur.execute(create_sql)
        
        # Copy data with column rename
        cur.execute("""
            INSERT INTO historical_prices 
            SELECT datetime, year, month, day, hour, minute, 
                   day_ahead_prices as ESIOS_600_DA_prices
            FROM spot_prices
        """)
        
        # Drop old table
        cur.execute("DROP TABLE spot_prices")
        
        conn.commit()
        print("✓ Successfully renamed table 'spot_prices' to 'historical_prices'")
        print("✓ Successfully renamed column 'day_ahead_prices' to 'ESIOS_600_DA_prices'")
        
    except Exception as e:
        conn.rollback()
        print(f"Error renaming table/column: {e}")
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    rename_table_and_column()

