from datetime import datetime, time, timedelta

import altair as alt
import pandas as pd
import streamlit as st

st.set_page_config(
    page_title="PV Captured Factor",
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

from captured_prices import (
    compute_captured_price_aggregations,
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


def compute_captured_factor_aggregations(
    joined_df: pd.DataFrame,
    prices_df: pd.DataFrame,
    group_cols: list[str]
) -> pd.DataFrame:
    """
    Compute PV-weighted captured factor over arbitrary groupings.
    
    captured_factor = PV_captured_price / baseload_price
    where:
    - PV_captured_price = sum(price * pv_mwh) / sum(pv_mwh) for the group
    - baseload_price = mean(price) for the same group
    
    Returns a DataFrame with group_cols and captured_factor column.
    """
    if joined_df.empty or prices_df.empty:
        return pd.DataFrame(columns=group_cols + ["captured_factor"])
    
    # Calculate PV captured price for the group
    captured_price_df = compute_captured_price_aggregations(joined_df, group_cols)
    
    # Calculate baseload price (average price) for the same group
    # Need to merge on the group columns to ensure same grouping
    baseload_price_df = prices_df.groupby(group_cols, as_index=False)["price_eur_per_mwh"].mean()
    baseload_price_df = baseload_price_df.rename(columns={"price_eur_per_mwh": "baseload_price"})
    
    # Merge captured price and baseload price
    merged = captured_price_df.merge(baseload_price_df, on=group_cols, how="inner")
    
    # Calculate captured factor
    merged["captured_factor"] = merged["captured_price"] / merged["baseload_price"]
    
    return merged[group_cols + ["captured_factor"]]


def main() -> None:
    # Title will be set after market selection to show inflation info
    title = "PV Captured Factor"

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
        key="pv_captured_factor_market_selector",
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
    st.title("PV Captured Factor")
    
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
    This page shows the PV captured factor, which represents the ratio of PV-weighted captured price to baseload (average) price.
    
    **Captured Factor = PV Captured Price / Baseload Price**
    
    - A factor of 1.0 means PV captures the same price as the baseload average
    - A factor > 1.0 means PV captures a premium (higher prices during PV generation)
    - A factor < 1.0 means PV captures a discount (lower prices during PV generation)
    """)

    # PV-weighted captured factor aggregations
    # Yearly
    st.subheader(get_chart_title("yearly", "pv_captured_factor"))
    yearly = compute_captured_factor_aggregations(joined, prices, ["year"])
    if not yearly.empty:
        chart = create_yearly_chart(yearly, "captured_factor", "Factor", show_labels=True)
        st.altair_chart(chart, use_container_width=True)

    # Year-month
    st.subheader(get_chart_title("year_month", "pv_captured_factor"))
    ym = joined.copy()
    ym["year_month"] = ym["datetime"].dt.to_period("M").dt.to_timestamp()
    ym_prices = prices.copy()
    ym_prices["year_month"] = ym_prices["datetime"].dt.to_period("M").dt.to_timestamp()
    ym_agg = compute_captured_factor_aggregations(ym, ym_prices, ["year_month"])
    if not ym_agg.empty:
        chart = create_year_month_chart(ym_agg, "captured_factor", "Factor", show_labels=False)
        st.altair_chart(chart, use_container_width=True)

    # Calendar-month
    st.subheader(get_chart_title("calendar_month", "pv_captured_factor"))
    cal = joined.copy()
    cal["month"] = cal["datetime"].dt.month
    cal_prices = prices.copy()
    cal_prices["month"] = cal_prices["datetime"].dt.month
    cal_agg = compute_captured_factor_aggregations(cal, cal_prices, ["month"])
    if not cal_agg.empty:
        first_year = cal["datetime"].dt.year.min() if not cal.empty else 2000
        cal_agg = ensure_all_months(cal_agg, "month")
        cal_agg["month_label"] = cal_agg["month"].apply(
            lambda m: datetime(first_year, m, 1).strftime("%b")
        )
        chart = create_calendar_month_chart(cal_agg, "captured_factor", "Factor", "month_label", show_labels=True)
        st.altair_chart(chart, use_container_width=True)

    # Daily
    st.subheader(get_chart_title("daily", "pv_captured_factor"))
    daily_avg = joined.copy()
    daily_avg["date"] = daily_avg["datetime"].dt.date
    daily_prices = prices.copy()
    daily_prices["date"] = daily_prices["datetime"].dt.date
    daily_agg = compute_captured_factor_aggregations(daily_avg, daily_prices, ["date"])
    if not daily_agg.empty:
        daily_agg["date_dt"] = pd.to_datetime(daily_agg["date"])
        chart = create_daily_chart(daily_agg, "captured_factor", "Factor", "date_dt")
        st.altair_chart(chart, use_container_width=True)

    # Day-of-week
    st.subheader(get_chart_title("day_of_week", "pv_captured_factor"))
    dow = joined.copy()
    dow["weekday"] = dow["datetime"].dt.day_name()
    dow["weekday_order"] = dow["datetime"].dt.weekday   # Monday=0 … Sunday=6
    dow_prices = prices.copy()
    dow_prices["weekday"] = dow_prices["datetime"].dt.day_name()
    dow_prices["weekday_order"] = dow_prices["datetime"].dt.weekday
    dow_agg = compute_captured_factor_aggregations(dow, dow_prices, ["weekday", "weekday_order"])
    if not dow_agg.empty:
        dow_agg = ensure_all_days(dow_agg, "weekday", "weekday_order")
        chart = create_day_of_week_chart(dow_agg, "captured_factor", "Factor", "weekday", show_labels=True)
        st.altair_chart(chart, use_container_width=True)

    # Hour-of-day
    st.subheader(get_chart_title("hour_of_day", "pv_captured_factor"))
    hod = joined.copy()
    hod["hour"] = hod["datetime"].dt.hour
    hod_prices = prices.copy()
    hod_prices["hour"] = hod_prices["datetime"].dt.hour
    hod_agg = compute_captured_factor_aggregations(hod, hod_prices, ["hour"])
    if not hod_agg.empty:
        hod_agg = ensure_all_hours(hod_agg, "hour")
        chart = create_hour_of_day_chart(hod_agg, "captured_factor", "Factor", "hour", show_labels=True)
        st.altair_chart(chart, use_container_width=True)

    # Raw captured-factor data and download
    st.subheader("Raw PV captured factor data")
    # Calculate captured factor for each row (for raw data display)
    # For raw data, we'll show the ratio at each point in time
    # This is approximate since factor is really an aggregate metric
    joined_display = joined.copy()
    # Calculate baseload price for the entire period
    overall_baseload = prices["price_eur_per_mwh"].mean()
    # For each row, captured price is just the price (since it's per-hour)
    joined_display["captured_price"] = joined_display["price_eur_per_mwh"]
    joined_display["baseload_price"] = overall_baseload
    joined_display["captured_factor"] = joined_display["captured_price"] / joined_display["baseload_price"]
    
    cols = [
        "datetime",
        "price_eur_per_mwh",
        "pv_mwh",
        "captured_price",
        "baseload_price",
        "captured_factor",
        "year",
        "month",
        "day",
        "hour",
        "weekday",
    ]
    available_cols = [c for c in cols if c in joined_display.columns]
    st.dataframe(joined_display[available_cols], use_container_width=True, height=400)
    csv = joined_display[available_cols].to_csv(index=False).encode("utf-8")
    st.download_button(
        label="Download PV captured factor data as CSV",
        data=csv,
        file_name=f"pv_captured_factor_{market}_{profile}.csv",
        mime="text/csv",
    )


# Streamlit automatically calls this when the page is loaded
main()

