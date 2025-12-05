"""
Rename forecast source values in the forecasts table.

Renames:
- "baringa" → "Baringa_Q2_2025"
- "aurora" → "Aurora_Jun_2025"
"""
import sqlite3
from pathlib import Path

DB_PATH = Path("data") / "data.db"


def rename_forecast_sources():
    """Rename forecast source values in the forecasts table."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    
    # Check if forecasts table exists
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='forecasts'")
    if cur.fetchone() is None:
        print("Table 'forecasts' does not exist. Nothing to rename.")
        conn.close()
        return
    
    # Check current source values
    cur.execute("SELECT DISTINCT source FROM forecasts")
    current_sources = [row[0] for row in cur.fetchall()]
    print(f"Current source values: {current_sources}")
    
    # Count rows to be updated
    cur.execute('SELECT COUNT(*) FROM forecasts WHERE source = "baringa"')
    baringa_count = cur.fetchone()[0]
    
    cur.execute('SELECT COUNT(*) FROM forecasts WHERE source = "aurora"')
    aurora_count = cur.fetchone()[0]
    
    print(f"\nRows to update:")
    print(f"  'baringa' → 'Baringa_Q2_2025': {baringa_count} rows")
    print(f"  'aurora' → 'Aurora_Jun_2025': {aurora_count} rows")
    
    if baringa_count == 0 and aurora_count == 0:
        print("\nNo rows to update.")
        conn.close()
        return
    
    # Update baringa
    if baringa_count > 0:
        cur.execute(
            'UPDATE forecasts SET source = ? WHERE source = ?',
            ("Baringa_Q2_2025", "baringa")
        )
        print(f"✓ Updated {baringa_count} rows: 'baringa' → 'Baringa_Q2_2025'")
    
    # Update aurora
    if aurora_count > 0:
        cur.execute(
            'UPDATE forecasts SET source = ? WHERE source = ?',
            ("Aurora_Jun_2025", "aurora")
        )
        print(f"✓ Updated {aurora_count} rows: 'aurora' → 'Aurora_Jun_2025'")
    
    conn.commit()
    
    # Verify
    cur.execute("SELECT DISTINCT source FROM forecasts")
    new_sources = [row[0] for row in cur.fetchall()]
    print(f"\nNew source values: {new_sources}")
    
    conn.close()
    print("\n✓ Successfully renamed forecast sources in database")


if __name__ == "__main__":
    rename_forecast_sources()

