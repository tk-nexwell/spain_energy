import sqlite3
from esios_api.fetch_prices import fetch_day_ahead_prices
from db.schema import create_price_table
from db.insert import insert_prices

def run_pipeline(start_date: str, end_date: str):
    conn = sqlite3.connect("data/prices.db")
    create_price_table(conn)

    indicator_id = 1001  # day-ahead prices
    df = fetch_day_ahead_prices(indicator_id, start=start_date, end=end_date)
    insert_prices(conn, df)

    conn.close()
