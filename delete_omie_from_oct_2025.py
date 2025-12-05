"""
Delete OMIE data from October 2025 onwards to allow re-upload with correct 15-minute parsing.
"""
import sqlite3
from datetime import datetime
from pathlib import Path

from db import DB_PATH


def delete_omie_from_oct_2025():
    """Delete OMIE_DA_prices data from October 1, 2025 onwards."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    
    # Delete OMIE prices from Oct 1, 2025 onwards
    # Set OMIE_DA_prices to NULL for those dates
    cutoff_date = "2025-10-01 00:00:00"
    
    print(f"Deleting OMIE data from {cutoff_date} onwards...")
    
    cur.execute(
        """
        UPDATE historical_prices 
        SET OMIE_DA_prices = NULL 
        WHERE datetime >= ?
        """,
        (cutoff_date,)
    )
    
    rows_affected = cur.rowcount
    conn.commit()
    conn.close()
    
    print(f"✓ Deleted OMIE data from {rows_affected} rows (set to NULL)")
    print(f"  Data from {cutoff_date} onwards will be re-uploaded with correct 15-minute parsing")


if __name__ == "__main__":
    delete_omie_from_oct_2025()

