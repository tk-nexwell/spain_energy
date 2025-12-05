"""
Delete rows from historical_prices table where all three price columns are NULL.

Deletes rows where:
- OMIE_SP_DA_prices IS NULL
- OMIE_PT_DA_prices IS NULL
- ESIOS_600_DA_prices IS NULL
"""
import sqlite3
from pathlib import Path

DATA_DIR = Path("data")
DB_PATH = DATA_DIR / "data.db"


def delete_empty_rows():
    """Delete rows from historical_prices where all price columns are NULL."""
    if not DB_PATH.exists():
        print("Database not found. Nothing to delete.")
        return
    
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    
    # Check if historical_prices table exists
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='historical_prices'")
    if not cur.fetchone():
        print("historical_prices table does not exist. Nothing to delete.")
        conn.close()
        return
    
    # Check which columns exist
    cur.execute("PRAGMA table_info(historical_prices)")
    cols = [c[1] for c in cur.fetchall()]
    
    # Verify required columns exist
    required_cols = ["OMIE_SP_DA_prices", "OMIE_PT_DA_prices", "ESIOS_600_DA_prices"]
    missing_cols = [col for col in required_cols if col not in cols]
    
    if missing_cols:
        print(f"Warning: Some columns are missing: {missing_cols}")
        print("Will only check columns that exist.")
    
    # Build WHERE clause based on available columns
    conditions = []
    for col in required_cols:
        if col in cols:
            conditions.append(f"{col} IS NULL")
    
    if not conditions:
        print("No price columns found. Nothing to delete.")
        conn.close()
        return
    
    # Count rows to be deleted
    where_clause = " AND ".join(conditions)
    cur.execute(f"SELECT COUNT(*) FROM historical_prices WHERE {where_clause}")
    count_to_delete = cur.fetchone()[0]
    
    if count_to_delete == 0:
        print("No rows found where all price columns are NULL. Nothing to delete.")
        conn.close()
        return
    
    print(f"Found {count_to_delete} rows where all price columns are NULL.")
    
    # Get total row count before deletion
    cur.execute("SELECT COUNT(*) FROM historical_prices")
    total_before = cur.fetchone()[0]
    
    # Confirm deletion
    response = input(f"Delete {count_to_delete} rows? (yes/no): ")
    if response.lower() != "yes":
        print("Deletion cancelled.")
        conn.close()
        return
    
    # Delete rows
    cur.execute(f"DELETE FROM historical_prices WHERE {where_clause}")
    rows_deleted = cur.rowcount
    
    conn.commit()
    
    # Get total row count after deletion
    cur.execute("SELECT COUNT(*) FROM historical_prices")
    total_after = cur.fetchone()[0]
    
    conn.close()
    
    print(f"✓ Successfully deleted {rows_deleted} rows.")
    print(f"  Total rows before: {total_before}")
    print(f"  Total rows after: {total_after}")
    print("Cleanup complete!")


if __name__ == "__main__":
    delete_empty_rows()
