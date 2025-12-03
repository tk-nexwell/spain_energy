import sqlite3
from pathlib import Path
from typing import Optional

import pandas as pd


DATA_DIR = Path("data")
DB_PATH = DATA_DIR / "spot_prices.db"


def get_table_name(indicator_id: int) -> str:
    """
    Return the SQLite table name for a given indicator.

    We keep the existing table `spot_prices` for indicator 600 for backwards
    compatibility, and use a separate table for other indicators (e.g. 1001).
    """
    if indicator_id == 600:
        return "spot_prices"
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
    df_to_write = df.copy()
    df_to_write["datetime"] = df_to_write["datetime"].astype(str)

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


