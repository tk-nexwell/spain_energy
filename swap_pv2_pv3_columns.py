"""
Swap the column names pv2 and pv3 in the pv_profiles table.
"""
import sqlite3
from pathlib import Path

from db import DB_PATH


def swap_pv2_pv3():
    """Swap pv2 and pv3 column names in pv_profiles table."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    
    # Check if table exists
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='pv_profiles'")
    if cur.fetchone() is None:
        print("Table 'pv_profiles' does not exist.")
        conn.close()
        return
    
    # Check if columns exist
    cur.execute("PRAGMA table_info(pv_profiles)")
    columns = [col[1] for col in cur.fetchall()]
    
    if "pv2" not in columns or "pv3" not in columns:
        print("Columns pv2 and/or pv3 do not exist in pv_profiles table.")
        conn.close()
        return
    
    print("Swapping pv2 and pv3 columns in pv_profiles table...")
    
    # SQLite doesn't support direct column renaming, so we need to recreate the table
    # Get all column info
    cur.execute("PRAGMA table_info(pv_profiles)")
    cols_info = cur.fetchall()
    
    # Build new column definitions, swapping pv2 and pv3
    new_cols_defs = []
    select_cols = []
    pk_cols = []
    
    for col in cols_info:
        col_name = col[1]
        col_type = col[2]
        is_pk = col[5] > 0
        
        if col_name == "pv2":
            # pv2 becomes pv3
            new_cols_defs.append(f"pv3 {col_type}")
            select_cols.append("pv2 AS pv3")
        elif col_name == "pv3":
            # pv3 becomes pv2
            new_cols_defs.append(f"pv2 {col_type}")
            select_cols.append("pv3 AS pv2")
        else:
            new_cols_defs.append(f"{col_name} {col_type}")
            select_cols.append(col_name)
            if is_pk:
                pk_cols.append(col_name)
    
    # Add composite primary key if there are PK columns
    if pk_cols:
        new_cols_defs.append(f"PRIMARY KEY ({', '.join(pk_cols)})")
    
    new_cols_def_str = ", ".join(new_cols_defs)
    select_cols_str = ", ".join(select_cols)
    
    # Create new table with swapped columns
    cur.execute(f"CREATE TABLE pv_profiles_new ({new_cols_def_str})")
    cur.execute(f"INSERT INTO pv_profiles_new SELECT {select_cols_str} FROM pv_profiles")
    cur.execute("DROP TABLE pv_profiles")
    cur.execute("ALTER TABLE pv_profiles_new RENAME TO pv_profiles")
    
    conn.commit()
    conn.close()
    
    print("✓ Successfully swapped pv2 and pv3 columns")
    print("  pv2 → pv3")
    print("  pv3 → pv2")


if __name__ == "__main__":
    swap_pv2_pv3()

