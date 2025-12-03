import os
import re
import sqlite3
from pathlib import Path

import pandas as pd


BASE_DIR = Path(__file__).resolve().parent
PV_DIR = BASE_DIR / "pv_prod"
DB_PATH = BASE_DIR / "data" / "pv.db"
PROFILES_TABLE = "pv_profiles"


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


def ensure_profiles_table(conn: sqlite3.Connection) -> None:
    """Create the unified PV profiles table if it does not exist."""
    cur = conn.cursor()
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS "{PROFILES_TABLE}" (
            month INTEGER,
            day INTEGER,
            hour INTEGER,
            PRIMARY KEY (month, day, hour)
        )
        """
    )
    conn.commit()


def ensure_profile_column(conn: sqlite3.Connection, column: str) -> None:
    """
    Ensure the given profile column (e.g. 'pv1', 'pv2') exists on the unified table.
    """
    cur = conn.cursor()
    cur.execute(f'PRAGMA table_info("{PROFILES_TABLE}")')
    cols = [row[1] for row in cur.fetchall()]
    if column not in cols:
        cur.execute(f'ALTER TABLE "{PROFILES_TABLE}" ADD COLUMN "{column}" REAL')
        conn.commit()


def load_single_file(path: Path, conn: sqlite3.Connection) -> None:
    """Load one PVSyst CSV into the unified PV profiles table."""
    profile_col = sanitize_table_name(path.name)
    print(f"Processing {path.name} -> profile column '{profile_col}'")

    # Find the header line dynamically: first line whose first field is 'date'.
    header_idx = None
    header_line = None
    with path.open("r", encoding="utf-8") as f:
        for i, line in enumerate(f):
            first_field = line.split(",")[0].strip().lower()
            if first_field == "date":
                header_idx = i
                header_line = line
                break

    if header_idx is None or header_line is None:
        print(f"  Skipping {path.name}: could not find header line starting with 'date'.")
        return

    # Build explicit column names from the detected header line and read the data rows.
    cols = [c.strip() for c in header_line.strip().split(",")]
    try:
        df = pd.read_csv(
            path,
            skiprows=header_idx + 1,
            names=cols,
            skipinitialspace=True,
        )
    except Exception as e:
        print(f"  Failed to read {path.name}: {e}")
        return

    # Parse date as DD/MM/YYYY HH:MM or DD/MM/YY HH:MM, ignoring the fake year.
    # dayfirst=True handles both 1/1/1990 and 01/01/90 variants.
    if "date" not in df.columns:
        print(f"  Skipping {path.name}: no 'date' column found.")
        return

    dt = pd.to_datetime(df["date"], dayfirst=True, errors="coerce")
    mask = dt.notna()
    df = df.loc[mask].copy()
    dt = dt.loc[mask]

    if df.empty:
        print(f"  No valid rows after parsing dates in {path.name}.")
        return

    df["month"] = dt.dt.month
    df["day"] = dt.dt.day
    df["hour"] = dt.dt.hour

    # Detect E_Grid column robustly, even if case/spacing differ.
    def norm(name: str) -> str:
        return re.sub(r"[^0-9a-zA-Z]+", "", str(name).strip().lower())

    norm_cols = {norm(c): c for c in df.columns}
    egrid_src = norm_cols.get("egrid")
    if egrid_src is None:
        print(
            f"  Skipping {path.name}: 'E_Grid' column not found in columns {list(df.columns)}"
        )
        return

    out = pd.DataFrame(
        {
            "month": df["month"].astype(int),
            "day": df["day"].astype(int),
            "hour": df["hour"].astype(int),
            "e_grid": df[egrid_src].astype(float),
        }
    )

    ensure_profiles_table(conn)
    ensure_profile_column(conn, profile_col)
    cur = conn.cursor()

    # Replace pandas NA with plain None so sqlite3 can bind parameters.
    df_clean = out.where(out.notna(), None)
    rows = list(df_clean.itertuples(index=False, name=None))

    # First ensure base (month,day,hour) rows exist
    cur.executemany(
        f"""
        INSERT OR IGNORE INTO "{PROFILES_TABLE}" (month, day, hour)
        VALUES (?, ?, ?)
        """,
        [(m, d, h) for m, d, h, _ in rows],
    )

    # Then update the specific profile column with e_grid values
    cur.executemany(
        f"""
        UPDATE "{PROFILES_TABLE}"
        SET "{profile_col}" = ?
        WHERE month = ? AND day = ? AND hour = ?
        """,
        [(e, m, d, h) for m, d, h, e in rows],
    )
    conn.commit()

    print(f"  Inserted/updated {len(rows)} hourly rows for profile '{profile_col}'.")


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


