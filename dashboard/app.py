import streamlit as st
import pandas as pd
import sqlite3

# Load data from SQLite
@st.cache_data
def load_data():
    conn = sqlite3.connect("data/prices.db")
    df = pd.read_sql("SELECT * FROM day_ahead_prices ORDER BY datetime", conn, parse_dates=["datetime"])
    conn.close()
    return df

# Load
df = load_data()

# Sidebar filter
st.sidebar.title("Filters")
days = st.sidebar.slider("How many recent days?", min_value=1, max_value=60, value=7)
cutoff = df["datetime"].max() - pd.Timedelta(days=days)
filtered = df[df["datetime"] >= cutoff]

# Title
st.title("🇪🇸 Spanish Day-Ahead Electricity Prices")
st.markdown(f"Showing last **{days} days** of hourly prices for Península")

# Line chart
st.line_chart(filtered.set_index("datetime")["price_eur_per_mwh"])

# Table
with st.expander("See raw data"):
    st.dataframe(filtered, use_container_width=True)
