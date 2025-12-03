import sqlite3
from datetime import datetime, time

import pandas as pd
import streamlit as st
import altair as alt

from config import INDICATORS
from db import DB_PATH, get_table_name


def _parse_timestamp(ts: str):
    """
    Parse timestamp strings from the DB into naive datetimes
    without shifting the wall-clock time.

    Examples:
    - '2015-01-01T01:00:00+01:00' -> datetime(2015, 1, 1, 1, 0, 0)
    - '2015-01-01T00:00:00Z' -> datetime(2015, 1, 1, 0, 0, 0)
    """
    s = str(ts)
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return None
    # Drop tzinfo so we can safely compare with naive datetimes from Streamlit.
    return dt.replace(tzinfo=None)


def load_data(indicator_id: int) -> pd.DataFrame:
    """Load all spot prices for a given indicator from the local SQLite database."""
    conn = sqlite3.connect(DB_PATH)
    try:
        table = get_table_name(indicator_id)
        df = pd.read_sql(f"SELECT * FROM {table} ORDER BY datetime", conn)
    finally:
        conn.close()

    if df.empty:
        return df

    # Build a helper naive datetime column for filtering/plotting; keep original string intact.
    parsed = df["datetime"].apply(_parse_timestamp)
    df = df[parsed.notna()].copy()
    df["datetime_parsed"] = parsed[parsed.notna()]
    return df


def main() -> None:
    st.set_page_config(page_title="Real-Time Prices (ESIOS 600)", layout="wide")

    st.sidebar.header("Real-time")
    indicator_id = st.sidebar.radio(
        "Indicator",
        options=list(INDICATORS.keys()),
        index=0,
        format_func=lambda k: f"{k} – {INDICATORS.get(k, str(k))}",
    )

    st.title(f"{INDICATORS.get(indicator_id, 'Prices')} Prices (ESIOS {indicator_id})")

    df = load_data(indicator_id)
    if df.empty:
        st.warning(
            "No data found in the database. "
            "Run `fetch_spot_to_csv.py` or `backfill_spot.py` first."
        )
        return

    min_ts = df["datetime_parsed"].min()
    max_ts = df["datetime_parsed"].max()
    min_date = min_ts.date()
    max_date = max_ts.date()

    # Default window: last 12 months (clamped to available data range)
    from datetime import timedelta

    default_start_date = max(max_date - timedelta(days=365), min_date)

    date_range = st.sidebar.slider(
        "Date range",
        min_value=min_date,
        max_value=max_date,
        value=(default_start_date, max_date),
        format="YYYY-MM-DD",
    )

    if isinstance(date_range, tuple):
        start_date, end_date = date_range
    else:
        start_date = end_date = date_range

    start_dt = datetime.combine(start_date, time.min)
    end_dt = datetime.combine(end_date, time.max)

    mask = (df["datetime_parsed"] >= start_dt) & (df["datetime_parsed"] <= end_dt)
    df_filtered = df.loc[mask].copy()

    st.subheader("Price time series")
    if df_filtered.empty:
        st.info("No data in the selected window.")
    else:
        chart_df = df_filtered[["datetime_parsed", "price_eur_per_mwh"]].copy()
        chart = (
            alt.Chart(chart_df)
            .mark_line()
            .encode(
                x=alt.X(
                    "datetime_parsed:T",
                    title="Datetime",
                    axis=alt.Axis(format="%Y-%m-%d", labelAngle=-45),
                ),
                y=alt.Y(
                    "price_eur_per_mwh:Q",
                    title="Price (€/MWh)",
                ),
                tooltip=[
                    alt.Tooltip("datetime_parsed:T", title="Datetime"),
                    alt.Tooltip("price_eur_per_mwh:Q", title="Price (€/MWh)"),
                ],
            )
            .properties(height=400)
            .interactive()
        )
        st.altair_chart(chart, use_container_width=True)

    if not df_filtered.empty:
        st.subheader("Yearly average price")
        yearly = (
            df_filtered.groupby("year", as_index=False)["price_eur_per_mwh"].mean()
        )
        yearly_base = (
            alt.Chart(yearly)
            .mark_bar()
            .encode(
                x=alt.X("year:O", title="Year"),
                y=alt.Y("price_eur_per_mwh:Q", title="Average price (€/MWh)"),
                tooltip=[
                    alt.Tooltip("year:O", title="Year"),
                    alt.Tooltip(
                        "price_eur_per_mwh:Q",
                        title="Avg price (€/MWh)",
                        format=".2f",
                    ),
                ],
            )
            .properties(height=250)
        )
        yearly_text = (
            alt.Chart(yearly)
            .mark_text(baseline="bottom", dy=-5)
            .encode(
                x="year:O",
                y="price_eur_per_mwh:Q",
                text=alt.Text("price_eur_per_mwh:Q", format=".1f"),
            )
        )
        st.altair_chart(yearly_base + yearly_text, use_container_width=True)

        st.subheader("Monthly average price (all months in window)")
        monthly = df_filtered.copy()
        monthly["year_month"] = (
            monthly["datetime_parsed"].dt.to_period("M").dt.to_timestamp()
        )
        monthly_agg = (
            monthly.groupby("year_month", as_index=False)["price_eur_per_mwh"].mean()
        )
        monthly_chart = (
            alt.Chart(monthly_agg)
            .mark_bar()
            .encode(
                x=alt.X(
                    "year_month:T",
                    title="Month",
                    axis=alt.Axis(format="%Y-%m", labelAngle=-45),
                ),
                y=alt.Y("price_eur_per_mwh:Q", title="Average price (€/MWh)"),
                tooltip=[
                    alt.Tooltip("year_month:T", title="Month", format="%Y-%m"),
                    alt.Tooltip(
                        "price_eur_per_mwh:Q",
                        title="Avg price (€/MWh)",
                        format=".2f",
                    ),
                ],
            )
            .properties(height=250)
        )
        st.altair_chart(monthly_chart, use_container_width=True)

        st.subheader("Average price by calendar month (Jan–Dec)")
        # Average each calendar month over years in the window
        month_year = df_filtered.copy()
        month_year["year"] = month_year["datetime_parsed"].dt.year
        month_year["month"] = month_year["datetime_parsed"].dt.month
        # mean price per (year, month)
        ym_price = (
            month_year.groupby(["year", "month"], as_index=False)["price_eur_per_mwh"]
            .mean()
            .rename(columns={"price_eur_per_mwh": "avg_price"})
        )
        # average over years for each calendar month
        cal_month = (
            ym_price.groupby("month", as_index=False)["avg_price"].mean().rename(
                columns={"avg_price": "avg_price_month"}
            )
        )
        cal_month["month_name"] = cal_month["month"].apply(
            lambda m: datetime(2000, m, 1).strftime("%b")
        )
        month_chart2 = (
            alt.Chart(cal_month)
            .mark_bar()
            .encode(
                x=alt.X("month_name:O", title="Calendar month"),
                y=alt.Y(
                    "avg_price_month:Q", title="Average price (€/MWh), per calendar month"
                ),
                tooltip=[
                    alt.Tooltip("month_name:O", title="Month"),
                    alt.Tooltip(
                        "avg_price_month:Q",
                        title="Avg price (€/MWh)",
                        format=".2f",
                    ),
                ],
            )
            .properties(height=250)
        )
        st.altair_chart(month_chart2, use_container_width=True)

        st.subheader("Daily average price")
        daily = df_filtered.copy()
        daily["date"] = daily["datetime_parsed"].dt.date
        daily_agg = (
            daily.groupby("date", as_index=False)["price_eur_per_mwh"].mean()
        )
        daily_chart = (
            alt.Chart(daily_agg)
            .mark_line()
            .encode(
                x=alt.X(
                    "date:T",
                    title="Date",
                    axis=alt.Axis(format="%Y-%m-%d", labelAngle=-45),
                ),
                y=alt.Y("price_eur_per_mwh:Q", title="Average price (€/MWh)"),
                tooltip=[
                    alt.Tooltip("date:T", title="Date", format="%Y-%m-%d"),
                    alt.Tooltip(
                        "price_eur_per_mwh:Q",
                        title="Avg price (€/MWh)",
                        format=".2f",
                    ),
                ],
            )
            .properties(height=250)
            .interactive()
        )
        st.altair_chart(daily_chart, use_container_width=True)

        st.subheader("Hourly average price (0–23)")
        hourly = (
            df_filtered.groupby("hour", as_index=False)["price_eur_per_mwh"].mean()
        )
        hourly_base = (
            alt.Chart(hourly)
            .mark_bar()
            .encode(
                x=alt.X("hour:O", title="Hour of day"),
                y=alt.Y("price_eur_per_mwh:Q", title="Average price (€/MWh)"),
                tooltip=[
                    alt.Tooltip("hour:O", title="Hour"),
                    alt.Tooltip(
                        "price_eur_per_mwh:Q",
                        title="Avg price (€/MWh)",
                        format=".2f",
                    ),
                ],
            )
            .properties(height=250)
        )
        hourly_text = (
            alt.Chart(hourly)
            .mark_text(baseline="bottom", dy=-5)
            .encode(
                x="hour:O",
                y="price_eur_per_mwh:Q",
                text=alt.Text("price_eur_per_mwh:Q", format=".1f"),
            )
        )
        st.altair_chart(hourly_base + hourly_text, use_container_width=True)

    st.subheader("Raw data")
    display_cols = [
        "datetime",
        "year",
        "month",
        "day",
        "hour",
        "minute",
        "price_eur_per_mwh",
    ]
    st.dataframe(
        df_filtered[display_cols],
        use_container_width=True,
        height=400,
    )

    # Download button for filtered data as CSV
    csv_bytes = df_filtered[display_cols].to_csv(index=False).encode("utf-8")
    st.download_button(
        label="Download filtered data as CSV",
        data=csv_bytes,
        file_name=f"esios_spot_{start_date}_{end_date}.csv",
        mime="text/csv",
    )


if __name__ == "__main__":
    main()


