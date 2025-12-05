"""
One-off script to import extra spot price indicator columns (612, 613, 614)
from data/spot_prices.db into the unified data/data.db spot_prices table.

Assumptions:
- data/data.db contains a table 'spot_prices' with at least:
    datetime TEXT PRIMARY KEY (or unique)
    ... and one base price column 'price_eur_per_mwh' for indicator 600
- data/spot_prices.db contains tables:
    spot_prices_612, spot_prices_613, spot_prices_614
  each with columns 'datetime' and 'price_eur_per_mwh'.

This script:
- Renames main spot_prices.price_eur_per_mwh -> day_ahead_prices
- Attaches data/spot_prices.db as 'old'
- Adds columns '612', '613', '614' (REAL) to main spot_prices if missing
- Copies price_eur_per_mwh from old.spot_prices_612/613/614 into those columns,
  matching on datetime.
"""

import os
import sqlite3

BASE = "data"
MAIN_DB = os.path.join(BASE, "data.db")
OLD_DB = os.path.join(BASE, "spot_prices.db")
TABLE = "spot_prices"


def main() -> None:
    if not os.path.exists(MAIN_DB):
        print(f"{MAIN_DB} does not exist.")
        return
    if not os.path.exists(OLD_DB):
        print(f"{OLD_DB} does not exist.")
        return

    conn = sqlite3.connect(MAIN_DB)
    cur = conn.cursor()

    # Rename base price column to day_ahead_prices (if not already renamed)
    cur.execute(f"PRAGMA table_info({TABLE})")
    main_info = cur.fetchall()
    main_cols = [r[1] for r in main_info]
    if "price_eur_per_mwh" in main_cols and "day_ahead_prices" not in main_cols:
        print("Renaming price_eur_per_mwh -> day_ahead_prices in main spot_prices...")
        try:
            cur.execute(
                f'ALTER TABLE "{TABLE}" RENAME COLUMN "price_eur_per_mwh" TO "day_ahead_prices"'
            )
            conn.commit()
            cur.execute(f"PRAGMA table_info({TABLE})")
            main_info = cur.fetchall()
            main_cols = [r[1] for r in main_info]
        except sqlite3.OperationalError as e:
            print("  Warning: could not rename column (maybe already renamed):", e)

    print(f"Attaching {OLD_DB} as 'old'...")
    cur.execute(f"ATTACH DATABASE '{OLD_DB}' AS old")

    # Ensure columns 612, 613, 614 exist in main spot_prices
    indicators = ["612", "613", "614"]
    for ind in indicators:
        if ind not in main_cols:
            print(f'Adding column "{ind}" to main spot_prices...')
            cur.execute(f'ALTER TABLE "{TABLE}" ADD COLUMN "{ind}" REAL')
    conn.commit()

    # For each indicator, update from the corresponding old.spot_prices_<ind> table
    for ind in indicators:
        src_table = f"spot_prices_{ind}"
        print(f"Importing indicator {ind} from old.{src_table} into {TABLE}...")
        # Check that the source table exists
        cur.execute(
            "SELECT name FROM old.sqlite_master WHERE type='table' AND name=?",
            (src_table,),
        )
        row = cur.fetchone()
        if not row:
            print(f"  Source table {src_table} not found in spot_prices.db, skipping.")
            continue

        # Update values by matching on datetime
        cur.execute(
            f"""
            UPDATE "{TABLE}" AS m
            SET "{ind}" = (
                SELECT o.price_eur_per_mwh
                FROM old.{src_table} AS o
                WHERE o.datetime = m.datetime
            )
            WHERE EXISTS (
                SELECT 1 FROM old.{src_table} AS o
                WHERE o.datetime = m.datetime
            )
            """
        )
        conn.commit()

    print("Detaching old DB...")
    cur.execute("DETACH DATABASE old")
    conn.close()
    print("Done importing indicators 612, 613, 614 into data.db spot_prices.")


if __name__ == "__main__":
    main()


