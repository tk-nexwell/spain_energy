"""
Script to remove the OMIE_DA_prices column from the historical_prices table.
Keeps OMIE_SP_DA_prices and OMIE_PT_DA_prices columns.
"""
import sqlite3
from pathlib import Path

DB_PATH = Path("data") / "data.db"


def remove_omie_da_column():
    """Remove OMIE_DA_prices column from historical_prices table."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    print("Checking historical_prices table structure...")
    
    # Get current column info
    cur.execute("PRAGMA table_info(historical_prices)")
    cols_info = cur.fetchall()
    
    col_names = [col[1] for col in cols_info]
    print(f"Current columns: {col_names}")
    
    if "OMIE_DA_prices" not in col_names:
        print("Column 'OMIE_DA_prices' does not exist. Nothing to remove.")
        conn.close()
        return
    
    if "OMIE_SP_DA_prices" not in col_names or "OMIE_PT_DA_prices" not in col_names:
        print("Warning: OMIE_SP_DA_prices or OMIE_PT_DA_prices not found.")
        print("Proceeding anyway...")
    
    print("\nRemoving OMIE_DA_prices column...")
    
    # Build new column definitions, excluding OMIE_DA_prices
    new_cols_defs = []
    select_cols = []
    pk_cols = []

    for col in cols_info:
        col_name = col[1]
        col_type = col[2]
        is_pk = col[5]  # 1 if part of PK, 0 otherwise
        
        if col_name == "OMIE_DA_prices":
            # Skip this column
            continue
        
        new_cols_defs.append(f"{col_name} {col_type}")
        select_cols.append(col_name)
        
        if is_pk:
            pk_cols.append(col_name)
    
    # Add primary key constraint if it exists
    if pk_cols:
        new_cols_defs.append(f"PRIMARY KEY ({', '.join(pk_cols)})")

    new_cols_def_str = ", ".join(new_cols_defs)
    select_cols_str = ", ".join(select_cols)

    # Rename original table
    cur.execute("ALTER TABLE historical_prices RENAME TO historical_prices_old")

    # Create new table without OMIE_DA_prices
    cur.execute(f"CREATE TABLE historical_prices ({new_cols_def_str})")

    # Copy data from old table to new table
    cur.execute(f"INSERT INTO historical_prices SELECT {select_cols_str} FROM historical_prices_old")

    # Drop old table
    cur.execute("DROP TABLE historical_prices_old")

    conn.commit()
    
    # Verify new structure
    cur.execute("PRAGMA table_info(historical_prices)")
    new_cols_info = cur.fetchall()
    new_col_names = [col[1] for col in new_cols_info]
    print(f"\nNew columns: {new_col_names}")
    
    if "OMIE_DA_prices" in new_col_names:
        print("ERROR: OMIE_DA_prices still exists!")
    else:
        print("✓ Successfully removed OMIE_DA_prices column")
    
    if "OMIE_SP_DA_prices" in new_col_names and "OMIE_PT_DA_prices" in new_col_names:
        print("✓ OMIE_SP_DA_prices and OMIE_PT_DA_prices columns preserved")
    else:
        print("WARNING: OMIE_SP_DA_prices or OMIE_PT_DA_prices missing!")
    
    conn.close()
    print("\n✓ Column removal complete")


if __name__ == "__main__":
    remove_omie_da_column()

