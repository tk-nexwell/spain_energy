from datetime import datetime, time, timedelta

import altair as alt
import pandas as pd
import streamlit as st

st.set_page_config(
    page_title="PPA Effective Prices",
    page_icon=":money_with_wings:",
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

from config import INDICATORS
from captured_prices import (
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
    """Map PV profile column names to display labels."""
    # Note: pv2 and pv3 columns have been swapped in the database
    mapping = {
        "pv1": "PV 1.2 DC/AC",
        "pv2": "PV 2.0 DC/AC",  # pv2 now contains what was pv3
        "pv3": "PV 1.5 DC/AC",  # pv3 now contains what was pv2
    }
    return mapping.get(name, name)


@st.cache_data
def compute_ppa_metrics(df: pd.DataFrame, strike: float) -> dict:
    """
    Compute PPA metrics from joined price and PV data.
    
    Returns a dictionary with:
    - eligible: Series of 0/1 for eligible settlement hours
    - ppa_revenue: Series of hourly PPA revenue
    - total_gen: Total PV generation (MWh)
    - total_rev: Total PPA revenue (€)
    - effective_price: Effective PPA price (€/MWh)
    - pct_eligible: Percentage of PV generation in eligible hours
    """
    if df.empty:
        return {
            "eligible": pd.Series(dtype=int),
            "ppa_revenue": pd.Series(dtype=float),
            "total_gen": 0.0,
            "total_rev": 0.0,
            "effective_price": 0.0,
            "pct_eligible": 0.0,
            "df": df.copy(),
        }
    
    # Eligible settlement hours (price > 0)
    df = df.copy()
    df["eligible"] = (df["price_eur_per_mwh"] > 0).astype(int)
    
    # PPA revenue = PV output * strike price * eligibility
    df["ppa_revenue"] = df["pv_mwh"] * strike * df["eligible"]
    
    # Total metrics
    total_gen = df["pv_mwh"].sum()
    total_rev = df["ppa_revenue"].sum()
    
    # Effective PPA price
    effective_price = total_rev / total_gen if total_gen > 0 else 0.0
    
    # Percentage of PV generation in eligible hours
    eligible_gen = df.loc[df["eligible"] == 1, "pv_mwh"].sum()
    pct_eligible = (eligible_gen / total_gen * 100) if total_gen > 0 else 0.0
    
    return {
        "eligible": df["eligible"],
        "ppa_revenue": df["ppa_revenue"],
        "total_gen": total_gen,
        "total_rev": total_rev,
        "effective_price": effective_price,
        "pct_eligible": pct_eligible,
        "df": df,  # Return the dataframe with computed columns
    }


def compute_ppa_effective_price_aggregations(df: pd.DataFrame, group_cols: list) -> pd.DataFrame:
    """
    Compute effective PPA price aggregations by grouping columns.
    
    For each group, calculates: effective_price = sum(ppa_revenue) / sum(pv_mwh)
    
    Args:
        df: DataFrame with columns: ppa_revenue, pv_mwh, and grouping columns
        group_cols: List of column names to group by
    
    Returns:
        DataFrame with group_cols and effective_price column
    """
    if df.empty or "ppa_revenue" not in df.columns or "pv_mwh" not in df.columns:
        return pd.DataFrame()
    
    grouped = df.groupby(group_cols, as_index=False).agg(
        total_revenue=("ppa_revenue", "sum"),
        total_pv=("pv_mwh", "sum"),
    )
    
    # Calculate effective price: revenue / generation
    grouped["effective_price"] = grouped.apply(
        lambda row: row["total_revenue"] / row["total_pv"] if row["total_pv"] > 0 else 0.0,
        axis=1,
    )
    
    # Return only grouping columns and effective_price
    result_cols = group_cols + ["effective_price"]
    return grouped[result_cols]


def compute_monthly_breakdown(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute monthly breakdown of PPA metrics.
    
    Returns DataFrame with columns:
    - month (1-12)
    - month_name (Jan-Dec)
    - total_pv (MWh)
    - eligible_pv (MWh)
    - pct_eligible (%)
    - revenue (€)
    - effective_price (€/MWh)
    """
    if df.empty or "month" not in df.columns:
        return pd.DataFrame(columns=["month", "month_name", "total_pv", "eligible_pv", "pct_eligible", "revenue", "effective_price"])
    
    # Group by month and compute aggregates
    monthly = df.groupby("month", as_index=False).agg(
        total_pv=("pv_mwh", "sum"),
        revenue=("ppa_revenue", "sum"),
    )
    
    # Calculate eligible PV for each month
    monthly["eligible_pv"] = 0.0
    for month in monthly["month"]:
        month_data = df[df["month"] == month]
        monthly.loc[monthly["month"] == month, "eligible_pv"] = month_data.loc[month_data["eligible"] == 1, "pv_mwh"].sum()
    
    monthly["pct_eligible"] = (monthly["eligible_pv"] / monthly["total_pv"] * 100).fillna(0.0)
    monthly["effective_price"] = (monthly["revenue"] / monthly["total_pv"]).fillna(0.0)
    
    # Add month names
    monthly["month_name"] = monthly["month"].apply(
        lambda m: datetime(2000, int(m), 1).strftime("%b")
    )
    
    # Ensure all 12 months are present
    all_months = pd.DataFrame({
        "month": list(range(1, 13)),
        "month_name": [datetime(2000, m, 1).strftime("%b") for m in range(1, 13)]
    })
    monthly = all_months.merge(monthly, on=["month", "month_name"], how="left")
    monthly = monthly.fillna(0.0)
    
    # Sort by month
    monthly = monthly.sort_values("month")
    
    return monthly[["month", "month_name", "total_pv", "eligible_pv", "pct_eligible", "revenue", "effective_price"]]


def main() -> None:
    # Title will be set after market selection to show inflation info
    title = "PPA Effective Prices"
    
    # Load market and PV profile lists
    markets = list_markets()
    pv_profiles = list_pv_profiles()
    
    if not markets:
        st.warning("No markets found in prices DB.")
        return
    if not pv_profiles:
        st.warning("No PV profiles found in PV DB.")
        return
    
    # Sidebar: Market selection (1)
    st.sidebar.header("Market")
    
    # Market selection using radio buttons (same as first page)
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
    
    if not market_ids:
        st.warning("No markets available.")
        return
    
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
        key="ppa_market_selector",
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
    
    # PPA strike price slider on main page
    strike_price = st.slider(
        "PPA strike price (€/MWh)",
        min_value=0.0,
        max_value=200.0,
        value=37.80,
        step=0.10,
        format="%.2f",
    )
    
    # Load prices to determine date range
    prices_all = load_price_series(market, inflation_rate=inflation_rate)
    if prices_all.empty:
        st.warning("No price data available for the selected market.")
        return
    
    # Load and join data
    prices = load_price_series(market, start_dt=start_dt, end_dt=end_dt, inflation_rate=inflation_rate)
    if prices.empty:
        st.warning("No price data available for the selected date range.")
        return
    
    try:
        pv = load_pv_profile(profile)
    except FileNotFoundError:
        st.warning("Missing PV profile.")
        return
    
    joined = join_price_with_pv(prices, pv)
    
    if joined.empty:
        st.info("No overlapping price and PV data for the selected combination.")
        return
    
    # Page header with standardized format
    st.title("PPA Effective Prices")
    
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
    This page calculates the effective price of an Amazon-style Power Purchase Agreement (PPA).
    
    **Key features:**
    - No settlement in hours where price ≤ 0 €/MWh
    - PPA revenue = PV output × strike price × eligibility
    - Effective price = Total revenue / Total PV generation
    
    The effective price represents the average price per MWh of PV generation, accounting for hours where
    the PPA does not settle due to negative or zero prices.
    """)
    
    # Compute PPA metrics
    metrics = compute_ppa_metrics(joined, strike_price)
    df_with_metrics = metrics["df"]
    
    # Display main metric
    st.subheader("Effective PPA Price")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric(
            "Effective PPA Price",
            f"{metrics['effective_price']:.2f} €/MWh",
            delta=None,
        )
    
    with col2:
        # Convert MWh to GWh and format with thousand separators
        total_gen_gwh = metrics['total_gen'] / 1000.0
        st.metric(
            "Total PV Generation",
            f"{total_gen_gwh:,.2f} GWh",
        )
    
    with col3:
        st.metric(
            "PV Generation in Eligible Hours",
            f"{metrics['pct_eligible']:.1f}%",
        )
    
    # Seasonality charts
    st.subheader("Effective PPA Price Seasonality")
    
    # Yearly
    st.subheader(get_chart_title("yearly", "ppa_effective"))
    yearly = df_with_metrics.copy()
    yearly_agg = compute_ppa_effective_price_aggregations(yearly, ["year"])
    if not yearly_agg.empty:
        chart = create_yearly_chart(yearly_agg, "effective_price", "€/MWh", show_labels=True)
        st.altair_chart(chart, use_container_width=True)
    
    # Year-month
    st.subheader(get_chart_title("year_month", "ppa_effective"))
    ym = df_with_metrics.copy()
    ym["year_month"] = ym["datetime"].dt.to_period("M").dt.to_timestamp()
    ym_agg = compute_ppa_effective_price_aggregations(ym, ["year_month"])
    if not ym_agg.empty:
        chart = create_year_month_chart(ym_agg, "effective_price", "€/MWh", show_labels=False)
        st.altair_chart(chart, use_container_width=True)
    
    # Calendar-month
    st.subheader(get_chart_title("calendar_month", "ppa_effective"))
    cal = df_with_metrics.copy()
    cal["month"] = cal["datetime"].dt.month
    cal_agg = compute_ppa_effective_price_aggregations(cal, ["month"])
    if not cal_agg.empty:
        first_year = cal["datetime"].dt.year.min() if not cal.empty else 2000
        cal_agg = ensure_all_months(cal_agg, "month")
        cal_agg["month_label"] = cal_agg["month"].apply(
            lambda m: datetime(first_year, m, 1).strftime("%b")
        )
        chart = create_calendar_month_chart(cal_agg, "effective_price", "€/MWh", "month_label", show_labels=True)
        st.altair_chart(chart, use_container_width=True)
    
    # Daily
    st.subheader(get_chart_title("daily", "ppa_effective"))
    daily_avg = df_with_metrics.copy()
    daily_avg["date"] = daily_avg["datetime"].dt.date
    daily_agg = compute_ppa_effective_price_aggregations(daily_avg, ["date"])
    if not daily_agg.empty:
        daily_agg["date_dt"] = pd.to_datetime(daily_agg["date"])
        chart = create_daily_chart(daily_agg, "effective_price", "€/MWh", "date_dt")
        st.altair_chart(chart, use_container_width=True)
    
    # Day-of-week
    st.subheader(get_chart_title("day_of_week", "ppa_effective"))
    dow = df_with_metrics.copy()
    dow["weekday"] = dow["datetime"].dt.day_name()
    dow["weekday_order"] = dow["datetime"].dt.weekday   # Monday=0 … Sunday=6
    dow_agg = compute_ppa_effective_price_aggregations(dow, ["weekday", "weekday_order"])
    if not dow_agg.empty:
        dow_agg = ensure_all_days(dow_agg, "weekday", "weekday_order")
        chart = create_day_of_week_chart(dow_agg, "effective_price", "€/MWh", "weekday", show_labels=True)
        st.altair_chart(chart, use_container_width=True)
    
    # Hour-of-day
    st.subheader(get_chart_title("hour_of_day", "ppa_effective"))
    hod = df_with_metrics.copy()
    hod["hour"] = hod["datetime"].dt.hour
    hod_agg = compute_ppa_effective_price_aggregations(hod, ["hour"])
    if not hod_agg.empty:
        hod_agg = ensure_all_hours(hod_agg, "hour")
        chart = create_hour_of_day_chart(hod_agg, "effective_price", "€/MWh", "hour", show_labels=True)
        st.altair_chart(chart, use_container_width=True)
    
    # Raw data download
    st.subheader("Raw Data")
    cols = [
        "datetime",
        "price_eur_per_mwh",
        "pv_mwh",
        "eligible",
        "ppa_revenue",
        "year",
        "month",
        "day",
        "hour",
    ]
    available_cols = [c for c in cols if c in df_with_metrics.columns]
    st.dataframe(df_with_metrics[available_cols], use_container_width=True, height=400)
    
    csv = df_with_metrics[available_cols].to_csv(index=False).encode("utf-8")
    st.download_button(
        label="Download PPA data as CSV",
        data=csv,
        file_name=f"ppa_effective_price_{market}_{profile}_{strike_price:.0f}.csv",
        mime="text/csv",
    )


# Streamlit automatically calls this when the page is loaded
main()

