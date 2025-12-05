import sqlite3
from pathlib import Path
from typing import Optional

import pandas as pd


DATA_DIR = Path("data")
DB_PATH = DATA_DIR / "data.db"


def get_table_name(indicator_id: int) -> str:
    """
    Return the SQLite table name for a given indicator.

    For indicator 600, use the historical_prices table.
    For other indicators, use separate tables (e.g. spot_prices_1001).
    """
    if indicator_id == 600:
        return "historical_prices"
    return f"spot_prices_{indicator_id}"


def init_db(indicator_id: int) -> None:
    """Create the SQLite database and table for the indicator if they do not exist."""
    DATA_DIR.mkdir(exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    table = get_table_name(indicator_id)
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table} (
            datetime TEXT PRIMARY KEY,
            year INTEGER,
            month INTEGER,
            day INTEGER,
            hour INTEGER,
            minute INTEGER,
            price_eur_per_mwh REAL
        )
        """
    )
    conn.commit()
    conn.close()


def get_latest_datetime(indicator_id: int) -> Optional[str]:
    """
    Return the latest datetime string stored for this indicator, or None if empty.
    """
    if not DB_PATH.exists():
        return None

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    table = get_table_name(indicator_id)
    try:
        cur.execute(f"SELECT MAX(datetime) FROM {table}")
    except sqlite3.OperationalError:
        # Table doesn't exist yet
        conn.close()
        return None

    row = cur.fetchone()
    conn.close()

    if row is None or row[0] is None:
        return None
    return str(row[0])


def insert_prices(df: pd.DataFrame, indicator_id: int) -> None:
    """
    Insert price rows into the database for a given indicator.

    Expects columns: datetime, year, month, day, hour, minute, price_eur_per_mwh.
    Uses INSERT OR REPLACE on the datetime primary key.
    """
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    table = get_table_name(indicator_id)

    # Ensure we are writing simple strings for the datetime column.
    # Standardize datetime format to "YYYY-MM-DD HH:MM:SS"
    df_to_write = df.copy()
    
    def standardize_datetime(ts):
        """Convert datetime to standardized format."""
        ts_str = str(ts)
        # Handle ISO format with T and Z
        if "T" in ts_str:
            ts_str = ts_str.replace("T", " ")
        if ts_str.endswith("Z"):
            ts_str = ts_str[:-1]
        if "+" in ts_str:
            ts_str = ts_str.split("+")[0]
        # Parse and reformat
        try:
            from datetime import datetime
            formats = [
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%dT%H:%M:%SZ",
                "%Y-%m-%d %H:%M:%S.%f",
                "%Y-%m-%dT%H:%M:%S.%fZ",
            ]
            for fmt in formats:
                try:
                    dt = datetime.strptime(ts_str, fmt)
                    return dt.strftime("%Y-%m-%d %H:%M:%S")
                except ValueError:
                    continue
            # Fallback to fromisoformat
            dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
            if dt.tzinfo:
                dt = dt.replace(tzinfo=None)
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            return ts_str  # Return original if parsing fails
    
    df_to_write["datetime"] = df_to_write["datetime"].apply(standardize_datetime)

    rows = list(
        df_to_write[
            [
                "datetime",
                "year",
                "month",
                "day",
                "hour",
                "minute",
                "price_eur_per_mwh",
            ]
        ].itertuples(index=False, name=None)
    )

    cur.executemany(
        f"""
        INSERT OR REPLACE INTO {table}
        (datetime, year, month, day, hour, minute, price_eur_per_mwh)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    conn.commit()
    conn.close()


