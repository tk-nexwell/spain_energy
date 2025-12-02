import sqlite3
import pandas as pd

conn = sqlite3.connect("data/prices.db")

# Look at last few prices
df = pd.read_sql("SELECT * FROM day_ahead_prices ORDER BY datetime DESC LIMIT 5", conn)
print(df)

conn.close()

