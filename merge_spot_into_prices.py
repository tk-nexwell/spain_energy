import sqlite3
import os

# Paths
base = "data"
prices = os.path.join(base, "prices.db")
spot = os.path.join(base, "spot_prices.db")

# Connect to prices.db (this will be your only database going forward)
conn = sqlite3.connect(prices)
cur = conn.cursor()

print("Attaching old spot database...")
cur.execute(f"ATTACH DATABASE '{spot}' AS spot")

print("\nCopying tables from spot_prices.db into prices.db...\n")

# Loop over all tables in the attached spot DB
for (table_name,) in cur.execute("SELECT name FROM spot.sqlite_master WHERE type='table'"):
    print(f"Copying table: {table_name}")

    # Create the table if not existing, copying full contents
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} AS
        SELECT * FROM spot.{table_name}
    """)

    # If table already existed, this will not insert new rows.
    # To *append* instead, replace the above with:
    # cur.execute(f"INSERT OR IGNORE INTO {table_name} SELECT * FROM spot.{table_name}")

# Save changes
conn.commit()

print("\nDetaching spot DB…")
cur.execute("DETACH DATABASE spot")

conn.close()
print("Done! spot_prices.db is now merged into prices.db.")

