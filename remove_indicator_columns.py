"""
Script to remove indicator columns 612, 613, 614 from the spot_prices table.
SQLite doesn't support DROP COLUMN in older versions, so we'll recreate the table.
"""
import sqlite3
from pathlib import Path

DB_PATH = Path("data") / "data.db"

def remove_indicator_columns():
    """Remove columns 612, 613, 614 from spot_prices table."""
    if not DB_PATH.exists():
        print(f"Database {DB_PATH} does not exist.")
        return
    
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    
    # Check if columns exist
    cur.execute("PRAGMA table_info(spot_prices)")
    cols = cur.fetchall()
    col_names = [c[1] for c in cols]
    
    indicators_to_remove = ["612", "613", "614"]
    existing_indicators = [ind for ind in indicators_to_remove if ind in col_names]
    
    if not existing_indicators:
        print("No indicator columns (612, 613, 614) found in spot_prices table.")
        conn.close()
        return
    
    print(f"Found indicator columns to remove: {existing_indicators}")
    
    # Get all columns except the ones we want to remove
    columns_to_keep = [c[1] for c in cols if c[1] not in indicators_to_remove]
    
    # Create new table without indicator columns
    print("Creating new table without indicator columns...")
    cur.execute("""
        CREATE TABLE spot_prices_new (
            datetime TEXT,
            year INT,
            month INT,
            day INT,
            hour INT,
            minute INT,
            day_ahead_prices REAL
        )
    """)
    
    # Copy data (only columns we're keeping)
    col_list = ", ".join(columns_to_keep)
    print(f"Copying data from old table (columns: {col_list})...")
    cur.execute(f"""
        INSERT INTO spot_prices_new ({col_list})
        SELECT {col_list}
        FROM spot_prices
    """)
    
    # Count rows
    cur.execute("SELECT COUNT(*) FROM spot_prices_new")
    row_count = cur.fetchone()[0]
    print(f"Copied {row_count} rows to new table.")
    
    # Drop old table
    print("Dropping old table...")
    cur.execute("DROP TABLE spot_prices")
    
    # Rename new table
    print("Renaming new table to spot_prices...")
    cur.execute("ALTER TABLE spot_prices_new RENAME TO spot_prices")
    
    conn.commit()
    conn.close()
    
    print("Successfully removed indicator columns 612, 613, 614 from spot_prices table.")


if __name__ == "__main__":
    remove_indicator_columns()

