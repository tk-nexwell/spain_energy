import os
import re
import sqlite3
from pathlib import Path

import pandas as pd


BASE_DIR = Path(__file__).resolve().parent
PV_DIR = BASE_DIR / "pv_prod"
DB_PATH = BASE_DIR / "data" / "prices.db"


def sanitize_table_name(filename: str) -> str:
    """
    Convert a filename to a safe SQLite table name:
    - lower case
    - strip extension
    - non-alphanumeric chars replaced with '_'
    """
    name = Path(filename).stem.lower()
    name = re.sub(r"[^0-9a-zA-Z]+", "_", name)
    name = name.strip("_")
    return name


def ensure_table(conn: sqlite3.Connection, table: str) -> None:
    """Create the per-file table if it does not already exist."""
    cur = conn.cursor()
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS "{table}" (
            month INTEGER,
            day INTEGER,
            hour INTEGER,
            e_grid REAL,
            e_array REAL,
            il_pmin REAL,
            il_pmax REAL,
            e_arr_mpp REAL,
            e_arr_nom REAL,
            PRIMARY KEY (month, day, hour)
        )
        """
    )
    conn.commit()


def load_single_file(path: Path, conn: sqlite3.Connection) -> None:
    """Load one PVSyst CSV into its own SQLite table."""
    table = sanitize_table_name(path.name)
    print(f"Processing {path.name} -> table '{table}'")

    # Read CSV, skipping the first 10 header lines so that the header row
    # with 'date,E_Grid,...' is read as the column header.
    try:
        df = pd.read_csv(
            path,
            skiprows=10,
        )
    except Exception as e:
        print(f"  Failed to read {path.name}: {e}")
        return

    if "date" not in df.columns:
        print(f"  Skipping {path.name}: no 'date' column found.")
        return

    # Parse date as DD/MM/YYYY HH:MM, ignoring the fake year.
    dt = pd.to_datetime(df["date"], format="%d/%m/%Y %H:%M", errors="coerce")
    mask = dt.notna()
    df = df.loc[mask].copy()
    dt = dt.loc[mask]

    if df.empty:
        print(f"  No valid rows after parsing dates in {path.name}.")
        return

    df["month"] = dt.dt.month
    df["day"] = dt.dt.day
    df["hour"] = dt.dt.hour

    # Map PVSyst columns to our schema
    col_map = {
        "E_Grid": "e_grid",
        "EArray": "e_array",
        "IL_Pmin": "il_pmin",
        "IL_Pmax": "il_pmax",
        "EArrMPP": "e_arr_mpp",
        "EArrNom": "e_arr_nom",
    }

    missing = [src for src in col_map if src not in df.columns]
    if missing:
        print(f"  Skipping {path.name}: missing expected columns {missing}")
        return

    df_renamed = df.assign(
        **{new: df[src] for src, new in col_map.items()}
    )[
        [
            "month",
            "day",
            "hour",
            "e_grid",
            "e_array",
            "il_pmin",
            "il_pmax",
            "e_arr_mpp",
            "e_arr_nom",
        ]
    ]

    ensure_table(conn, table)
    cur = conn.cursor()

    rows = list(df_renamed.itertuples(index=False, name=None))
    cur.executemany(
        f"""
        INSERT OR REPLACE INTO "{table}"
        (month, day, hour, e_grid, e_array, il_pmin, il_pmax, e_arr_mpp, e_arr_nom)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    conn.commit()

    print(f"  Inserted/updated {len(rows)} rows into '{table}'.")


def main() -> None:
    if not PV_DIR.exists():
        print(f"Folder '{PV_DIR}' does not exist. Nothing to do.")
        return

    csv_files = sorted(PV_DIR.glob("*.csv"))
    if not csv_files:
        print(f"No CSV files found in '{PV_DIR}'.")
        return

    if not DB_PATH.exists():
        print(f"SQLite DB '{DB_PATH}' does not exist. Creating it.")
        DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        DB_PATH.touch()

    conn = sqlite3.connect(DB_PATH)
    try:
        for path in csv_files:
            load_single_file(path, conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()


