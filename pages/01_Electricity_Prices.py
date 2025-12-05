from datetime import datetime, time

import pandas as pd
import streamlit as st
import altair as alt

st.set_page_config(
    page_title="Electricity Prices",
    page_icon=":chart_with_upwards_trend:",
    layout="wide"
)

# Display logo in sidebar
try:
    st.sidebar.image("NP_logo.svg", use_container_width=True)
except Exception:
    pass  # Logo file not found, continue without it

# Apply brand styling
from style_config import apply_brand_styling
apply_brand_styling()

from data_loader import DataSource, load_price_data
from session_state import get_data_source_selector, get_inflation_input, get_date_range_selector
from chart_config import (
    get_chart_title,
    create_yearly_chart,
    create_year_month_chart,
    create_calendar_month_chart,
    create_daily_chart,
    create_day_of_week_chart,
    create_hour_of_day_chart,
    ensure_all_months,
    ensure_all_days,
    ensure_all_hours,
    MONTH_ORDER,
)


def main() -> None:
    # Sidebar: Market selection (1)
    st.sidebar.header("Market")
    source = get_data_source_selector()
    
    # Sidebar: Date range (2)
    st.sidebar.header("Date Range")
    start_dt, end_dt = get_date_range_selector(source)
    
    # Sidebar: Inflation (4) - only for forecasts
    inflation_rate = get_inflation_input(source)
    
    # Load data with inflation adjustment for forecasts
    df = load_price_data(source, start_dt=start_dt, end_dt=end_dt, inflation_rate=inflation_rate)
    
    if df.empty:
        st.warning(
            "No data found in the database for the selected source and date range."
        )
        return
    
    # Page header with standardized format
    st.title("Electricity Prices")
    
    # Get market label
    from session_state import init_session_state
    init_session_state()
    data_sources = {
        "OMIE DA SP (historical)": "omie_da",
        "ESIOS DA 600 (historical)": "historical_prices",
        "Aurora June 2025 (forecast)": "Aurora_Jun_2025",
        "Baringa Q2 2025 (forecast)": "Baringa_Q2_2025",
    }
    market_label = None
    for label, src in data_sources.items():
        if src == source:
            market_label = label
            break
    if market_label is None:
        market_label = source
    
    # Format date range (d-mmm-yyyy) - Windows doesn't support %-d, so format manually
    def format_date_d_mmm_yyyy(dt):
        """Format date as d-mmm-yyyy (e.g., 5-Jan-2025)"""
        day = dt.day
        month_abbr = dt.strftime("%b")
        year = dt.year
        return f"{day}-{month_abbr}-{year}"
    
    start_date_str = format_date_d_mmm_yyyy(start_dt)
    end_date_str = format_date_d_mmm_yyyy(end_dt)
    
    # Display header information
    st.markdown(f"**Market:** {market_label}")
    st.markdown(f"**Date Range:** {start_date_str} to {end_date_str}")
    if source not in ("historical_prices", "omie_da") and inflation_rate > 0:
        st.markdown(f"**Inflation:** inflated at {inflation_rate*100:.1f}% p.a.")
    
    st.markdown("""
    This page displays electricity prices with various aggregations to analyze price patterns over time.
    Prices are shown in €/MWh and can be viewed by year, month, calendar month, day, day of week, and hour of day.
    """)

    # Yearly average prices
    st.subheader(get_chart_title("yearly", "prices"))
    yearly = (
        df.groupby("year", as_index=False)["price_eur_per_mwh"].mean()
    )
    if not yearly.empty:
        chart = create_yearly_chart(yearly, "price_eur_per_mwh", "€/MWh", show_labels=True)
        st.altair_chart(chart, use_container_width=True)

    # Monthly average prices (all months in period)
    st.subheader(get_chart_title("year_month", "prices"))
    monthly = df.copy()
    monthly["year_month"] = (
        monthly["datetime_parsed"].dt.to_period("M").dt.to_timestamp()
    )
    monthly_agg = (
        monthly.groupby("year_month", as_index=False)["price_eur_per_mwh"].mean()
    )
    if not monthly_agg.empty:
        chart = create_year_month_chart(monthly_agg, "price_eur_per_mwh", "€/MWh", show_labels=False)
        st.altair_chart(chart, use_container_width=True)

    # Calendar-month average prices (Jan–Dec)
    st.subheader(get_chart_title("calendar_month", "prices"))
    month_year = (
        df.groupby(["year", "month"], as_index=False)["price_eur_per_mwh"]
        .mean()
        .rename(columns={"price_eur_per_mwh": "avg_price"})
    )
    if not month_year.empty:
        # Aggregate across all years for each calendar month
        cal_month = (
            month_year.groupby("month", as_index=False)["avg_price"].mean()
        )
        # Ensure all 12 calendar months are present
        cal_month = ensure_all_months(cal_month, "month")
        cal_month["month_name"] = cal_month["month"].apply(
            lambda m: datetime(2000, m, 1).strftime("%b")
        )
        chart = create_calendar_month_chart(cal_month, "avg_price", "€/MWh", "month_name", show_labels=True)
        st.altair_chart(chart, use_container_width=True)

    # Daily average prices
    st.subheader(get_chart_title("daily", "prices"))
    daily_avg = df.copy()
    daily_avg["date"] = daily_avg["datetime_parsed"].dt.date
    daily_agg = (
        daily_avg.groupby("date", as_index=False)["price_eur_per_mwh"].mean()
    )
    if not daily_agg.empty:
        daily_agg["date_dt"] = pd.to_datetime(daily_agg["date"])
        chart = create_daily_chart(daily_agg, "price_eur_per_mwh", "€/MWh", "date_dt")
        st.altair_chart(chart, use_container_width=True)

    # Daily average prices (by day of week)
    st.subheader(get_chart_title("day_of_week", "prices"))
    daily_df = df.copy()
    daily_df["date"] = daily_df["datetime_parsed"].dt.date
    daily = (
        daily_df.groupby("date", as_index=False)["price_eur_per_mwh"].mean()
    )
    if not daily.empty:
        daily["weekday"] = pd.to_datetime(daily["date"]).dt.day_name()
        daily["weekday_order"] = pd.to_datetime(daily["date"]).dt.weekday  # Monday=0, Sunday=6
        daily_agg = daily.groupby(["weekday", "weekday_order"], as_index=False)["price_eur_per_mwh"].mean()
        daily_agg = ensure_all_days(daily_agg, "weekday", "weekday_order")
        chart = create_day_of_week_chart(daily_agg, "price_eur_per_mwh", "€/MWh", "weekday", show_labels=True)
        st.altair_chart(chart, use_container_width=True)

    # Hourly average prices (0–23)
    st.subheader(get_chart_title("hour_of_day", "prices"))
    hourly = (
        df.groupby("hour", as_index=False)["price_eur_per_mwh"].mean()
    )
    if not hourly.empty:
        hourly = ensure_all_hours(hourly, "hour")
        chart = create_hour_of_day_chart(hourly, "price_eur_per_mwh", "€/MWh", "hour", show_labels=True)
        st.altair_chart(chart, use_container_width=True)

    # Raw data and download
    st.subheader("Raw data")
    cols = [
        "datetime",
        "price_eur_per_mwh",
        "year",
        "month",
        "day",
        "hour",
    ]
    available_cols = [c for c in cols if c in df.columns]
    st.dataframe(df[available_cols], use_container_width=True, height=400)
    csv = df[available_cols].to_csv(index=False).encode("utf-8")
    start_date_str = start_dt.strftime("%Y-%m-%d")
    end_date_str = end_dt.strftime("%Y-%m-%d")
    st.download_button(
        label="Download data as CSV",
        data=csv,
        file_name=f"prices_{source}_{start_date_str}_{end_date_str}.csv",
        mime="text/csv",
    )


# Streamlit automatically calls this when the page is loaded
main()

