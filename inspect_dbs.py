import os
import sqlite3

BASE = "data"
data_path = os.path.join(BASE, "data.db")
pv_path = os.path.join(BASE, "pv.db")
spot_path = os.path.join(BASE, "spot_prices.db")

print("data.db exists:", os.path.exists(data_path))
print("pv.db exists:", os.path.exists(pv_path))
print("spot_prices.db exists:", os.path.exists(spot_path))

def show_db(path: str, label: str) -> None:
    if not os.path.exists(path):
        return
    conn = sqlite3.connect(path)
    cur = conn.cursor()
    print(f"\n[{label}] tables:")
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    tables = [r[0] for r in cur.fetchall()]
    print(tables)
    for t in tables:
        print(f"\n[{label}] schema for table {t}:")
        cur.execute(f"PRAGMA table_info({t})")
        for row in cur.fetchall():
            print(row)
    conn.close()

show_db(data_path, "data.db")
show_db(pv_path, "pv.db")
show_db(spot_path, "spot_prices.db")



