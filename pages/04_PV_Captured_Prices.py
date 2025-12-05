from datetime import datetime, time, timedelta

import altair as alt
import pandas as pd
import streamlit as st

st.set_page_config(
    page_title="PV Captured Prices",
    page_icon=":sunny:",
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

from captured_prices import (
    compute_captured_price_aggregations,
    compute_typical_day_profiles,
    join_price_with_pv,
    list_markets,
    list_pv_profiles,
    load_price_series,
    load_pv_profile,
)
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
)


def _profile_label(name: str) -> str:
    # Note: pv2 and pv3 columns have been swapped in the database
    mapping = {
        "pv1": "PV 1.2 DC/AC",
        "pv2": "PV 2.0 DC/AC",  # pv2 now contains what was pv3
        "pv3": "PV 1.5 DC/AC",  # pv3 now contains what was pv2
    }
    return mapping.get(name, name)


def main() -> None:
    # Title will be set after market selection to show inflation info
    title = "PV Captured Prices"

    markets = list_markets()
    pv_profiles = list_pv_profiles()

    if not markets:
        st.warning("No markets found in prices DB.")
        return
    if not pv_profiles:
        st.warning("No PV profiles found in PV DB.")
        return

    # Ensure markets are in standardized order
    # Order: 1) OMIE DA SP, 2) ESIOS DA 600, 3) Aurora, 4) Baringa
    standardized_order = ["omie_da", "600", "Aurora_Jun_2025", "Baringa_Q2_2025"]
    market_ids = []
    for market_id in standardized_order:
        if market_id in markets:
            market_ids.append(market_id)
    # Add any other markets that might exist (shouldn't happen, but be safe)
    for market_id in markets.keys():
        if market_id not in market_ids:
            market_ids.append(market_id)
    
    default_market = market_ids[0] if market_ids else None
    if default_market is None:
        st.warning("No markets available.")
        return

    # Sidebar: Market selection (1)
    st.sidebar.header("Market")
    
    # Map market IDs to data sources for session state
    market_to_source = {}
    for market_id in market_ids:
        if market_id == "600":
            market_to_source[market_id] = "historical_prices"
        elif market_id == "omie_da":
            market_to_source[market_id] = "omie_da"
        elif market_id in ["Aurora_Jun_2025", "Baringa_Q2_2025"]:
            market_to_source[market_id] = market_id
        else:
            market_to_source[market_id] = "historical_prices"  # Default to historical
    
    # Initialize session state
    from session_state import init_session_state
    init_session_state()
    
    # Find current market based on session state
    current_source = st.session_state.get("data_source", "historical_prices")
    current_market = None
    for market_id, source in market_to_source.items():
        if source == current_source:
            current_market = market_id
            break
    
    if current_market is None:
        current_market = market_ids[0]
    
    current_index = market_ids.index(current_market) if current_market in market_ids else 0
    
    market = st.sidebar.radio(
        "Market",
        options=market_ids,
        format_func=lambda k: markets[k].label,
        index=current_index,
        key="pv_captured_market_selector",
    )
    
    # Update session state when market changes
    if market in market_to_source:
        source_for_state = market_to_source[market]
        if st.session_state.get("data_source") != source_for_state:
            st.session_state.data_source = source_for_state
            # Reset date range when source changes
            st.session_state.date_range_start = None
            st.session_state.date_range_end = None
    
    # Sidebar: Date range (2)
    st.sidebar.header("Date Range")
    from data_loader import DataSource
    source: DataSource = market_to_source.get(market, "historical_prices")
    from session_state import get_date_range_selector
    start_dt, end_dt = get_date_range_selector(source)
    
    # Sidebar: PV profile (3)
    st.sidebar.header("PV Profile")
    profile = st.sidebar.selectbox(
        "PV profile",
        options=pv_profiles,
        format_func=_profile_label,
        index=0,
    )
    
    # Sidebar: Inflation (4) - only for forecasts
    from session_state import get_inflation_input
    inflation_rate = get_inflation_input(source)

    # Load prices to determine date range
    prices_all = load_price_series(market, inflation_rate=inflation_rate)
    if prices_all.empty:
        st.info("No price data available for the selected market.")
        return

    prices = load_price_series(market, start_dt=start_dt, end_dt=end_dt, inflation_rate=inflation_rate)
    pv = load_pv_profile(profile)
    joined = join_price_with_pv(prices, pv)

    if joined.empty:
        st.info("No overlapping price and PV data for the selected combination.")
        return
    
    # Page header with standardized format
    st.title("PV Captured Prices")
    
    # Get market label
    market_label = markets[market].label
    
    # Format date range (d-mmm-yyyy)
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
    st.markdown(f"**PV Profile:** {_profile_label(profile)}")
    if source not in ("historical_prices", "omie_da") and inflation_rate > 0:
        st.markdown(f"**Inflation:** inflated at {inflation_rate*100:.1f}% p.a.")
    
    st.markdown("""
    This page shows the PV-weighted average price captured by solar generation.
    The captured price represents the average price during hours when PV is generating,
    weighted by the amount of PV output in each hour.
    """)
    
    # For time series, captured price at each point is just the price (since it's per-hour)
    # But we keep it for consistency with aggregations
    joined["captured_price"] = joined["price_eur_per_mwh"]

    # PV-weighted captured price aggregations
    # Yearly
    st.subheader(get_chart_title("yearly", "pv_captured"))
    yearly = compute_captured_price_aggregations(joined, ["year"])
    if not yearly.empty:
        chart = create_yearly_chart(yearly, "captured_price", "€/MWh", show_labels=True)
        st.altair_chart(chart, use_container_width=True)

    # Year-month
    st.subheader(get_chart_title("year_month", "pv_captured"))
    ym = joined.copy()
    ym["year_month"] = ym["datetime"].dt.to_period("M").dt.to_timestamp()
    ym_agg = compute_captured_price_aggregations(ym, ["year_month"])
    if not ym_agg.empty:
        chart = create_year_month_chart(ym_agg, "captured_price", "€/MWh", show_labels=False)
        st.altair_chart(chart, use_container_width=True)

    # Calendar-month
    st.subheader(get_chart_title("calendar_month", "pv_captured"))
    cal = joined.copy()
    cal["month"] = cal["datetime"].dt.month
    cal_agg = compute_captured_price_aggregations(cal, ["month"])
    if not cal_agg.empty:
        first_year = cal["datetime"].dt.year.min() if not cal.empty else 2000
        cal_agg = ensure_all_months(cal_agg, "month")
        cal_agg["month_label"] = cal_agg["month"].apply(
            lambda m: datetime(first_year, m, 1).strftime("%b")
        )
        chart = create_calendar_month_chart(cal_agg, "captured_price", "€/MWh", "month_label", show_labels=True)
        st.altair_chart(chart, use_container_width=True)

    # Daily
    st.subheader(get_chart_title("daily", "pv_captured"))
    daily_avg = joined.copy()
    daily_avg["date"] = daily_avg["datetime"].dt.date
    daily_agg = compute_captured_price_aggregations(daily_avg, ["date"])
    if not daily_agg.empty:
        daily_agg["date_dt"] = pd.to_datetime(daily_agg["date"])
        chart = create_daily_chart(daily_agg, "captured_price", "€/MWh", "date_dt")
        st.altair_chart(chart, use_container_width=True)

    # Day-of-week
    st.subheader(get_chart_title("day_of_week", "pv_captured"))
    dow = joined.copy()
    dow["weekday"] = dow["datetime"].dt.day_name()
    dow["weekday_order"] = dow["datetime"].dt.weekday   # Monday=0 … Sunday=6
    dow_agg = compute_captured_price_aggregations(dow, ["weekday", "weekday_order"])
    if not dow_agg.empty:
        dow_agg = ensure_all_days(dow_agg, "weekday", "weekday_order")
        chart = create_day_of_week_chart(dow_agg, "captured_price", "€/MWh", "weekday", show_labels=True)
        st.altair_chart(chart, use_container_width=True)

    # Hour-of-day
    st.subheader(get_chart_title("hour_of_day", "pv_captured"))
    hod = joined.copy()
    hod["hour"] = hod["datetime"].dt.hour
    hod_agg = compute_captured_price_aggregations(hod, ["hour"])
    if not hod_agg.empty:
        hod_agg = ensure_all_hours(hod_agg, "hour")
        chart = create_hour_of_day_chart(hod_agg, "captured_price", "€/MWh", "hour", show_labels=True)
        st.altair_chart(chart, use_container_width=True)

    # Raw captured-price data and download
    st.subheader("Raw PV captured price data")
    cols = [
        "datetime",
        "price_eur_per_mwh",
        "pv_mwh",
        "pv_weighted_price_component",
        "captured_price",
        "year",
        "month",
        "day",
        "hour",
        "weekday",
    ]
    available_cols = [c for c in cols if c in joined.columns]
    st.dataframe(joined[available_cols], use_container_width=True, height=400)
    csv = joined[available_cols].to_csv(index=False).encode("utf-8")
    st.download_button(
        label="Download PV captured price data as CSV",
        data=csv,
        file_name=f"pv_captured_prices_{market}_{profile}.csv",
        mime="text/csv",
    )


# Streamlit automatically calls this when the page is loaded
main()


