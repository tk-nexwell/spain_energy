"""Verify that datetime standardization worked and OMIE columns exist."""
import sqlite3
from db import DB_PATH

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

# Check OMIE columns
cur.execute("PRAGMA table_info(historical_prices)")
cols = [c[1] for c in cur.fetchall()]
omie_cols = [c for c in cols if "OMIE" in c]
print("OMIE columns in historical_prices:", omie_cols)

# Check datetime format samples
cur.execute("SELECT datetime FROM historical_prices LIMIT 5")
samples = [r[0] for r in cur.fetchall()]
print("\nSample datetime formats:")
for s in samples:
    print(f"  {s}")

# Check for any non-standard formats
cur.execute("SELECT COUNT(*) FROM historical_prices WHERE datetime LIKE '%T%' OR datetime LIKE '%Z%'")
non_standard_count = cur.fetchone()[0]
print(f"\nNon-standard datetime formats remaining: {non_standard_count}")

# Check OMIE data counts
if "OMIE_SP_DA_prices" in cols:
    cur.execute("SELECT COUNT(*) FROM historical_prices WHERE OMIE_SP_DA_prices IS NOT NULL")
    sp_count = cur.fetchone()[0]
    print(f"\nOMIE_SP_DA_prices rows: {sp_count}")

if "OMIE_PT_DA_prices" in cols:
    cur.execute("SELECT COUNT(*) FROM historical_prices WHERE OMIE_PT_DA_prices IS NOT NULL")
    pt_count = cur.fetchone()[0]
    print(f"OMIE_PT_DA_prices rows: {pt_count}")

conn.close()

