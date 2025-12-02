import sqlite3

def create_price_table(conn):
    query = """
    CREATE TABLE IF NOT EXISTS day_ahead_prices (
        datetime TEXT PRIMARY KEY,
        price_eur_per_mwh REAL
    );
    """
    conn.execute(query)
    conn.commit()
