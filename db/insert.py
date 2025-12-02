def insert_prices(conn, df):
    # Convert datetime to ISO string (SQLite needs string, not Timestamp)
    df["datetime"] = df["datetime"].astype(str)

    query = """
    INSERT OR REPLACE INTO day_ahead_prices (datetime, price_eur_per_mwh)
    VALUES (?, ?)
    """
    data = list(df.to_records(index=False))
    conn.executemany(query, data)
    conn.commit()
