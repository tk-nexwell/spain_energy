from datetime import datetime, time, timedelta
from typing import List, Tuple, Optional

import altair as alt
import pandas as pd
import streamlit as st

st.set_page_config(
    page_title="BESS Spreads",
    page_icon=":battery:",
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
    BRAND_COLOR,
    get_chart_title,
    create_yearly_chart,
    create_year_month_chart,
    create_calendar_month_chart,
    create_daily_chart,
    create_day_of_week_chart,
    create_multi_series_bar_chart,
    create_multi_series_line_chart,
    ensure_all_months,
    ensure_all_days,
    MONTH_ORDER,
    DAY_ORDER,
)


def parse_time_input(time_str: str) -> time:
    """Parse time string in HH:MM format to time object."""
    try:
        hour, minute = map(int, time_str.split(":"))
        return time(hour, minute)
    except (ValueError, AttributeError):
        return time(8, 0)  # Default to 8 AM


def format_time(t: time) -> str:
    """Format time object to HH:MM string."""
    return f"{t.hour:02d}:{t.minute:02d}"


def time_input_with_arrows(label: str, default_time: time, key: str) -> time:
    """
    Create a time input with up/down arrow buttons for incrementing/decrementing hours only.
    Minutes are always set to 0 (top of the hour).
    Uses Streamlit's number_input which has built-in up/down arrows.
    
    Returns:
        time object representing the selected time (always at top of hour)
    """
    # Use session state to track the time
    if key not in st.session_state:
        st.session_state[key] = default_time
    
    current_time = st.session_state[key]
    
    # Only allow hour selection, minutes always 0
    hour = st.number_input(
        "Hour",
        min_value=0,
        max_value=23,
        value=current_time.hour,
        step=1,
        key=f"{key}_hour",
    )
    
    # Always set minutes to 0 (top of the hour)
    selected_time = time(hour, 0)
    st.session_state[key] = selected_time
    
    return selected_time


def validate_cycles(
    charge1: time, discharge1: time,
    charge2: Optional[time], discharge2: Optional[time]
) -> Tuple[bool, str]:
    """
    Validate that charge/discharge cycles don't overlap.
    
    Returns:
        (is_valid, error_message)
    """
    if charge2 is None or discharge2 is None:
        # Single cycle: just check charge is before discharge
        if charge1 >= discharge1:
            return False, "Charge time must be before discharge time for cycle 1."
        return True, ""
    
    # Two cycles: check all constraints
    if charge1 >= discharge1:
        return False, "Charge time must be before discharge time for cycle 1."
    if charge2 >= discharge2:
        return False, "Charge time must be before discharge time for cycle 2."
    
    # Check for overlaps
    times = [
        ("Charge 1", charge1),
        ("Discharge 1", discharge1),
        ("Charge 2", charge2),
        ("Discharge 2", discharge2),
    ]
    sorted_times = sorted(times, key=lambda x: (x[1].hour, x[1].minute))
    
    # Check if cycle 2 is completely before cycle 1
    if discharge2 <= charge1:
        return True, ""
    
    # Check if cycle 2 is completely after cycle 1
    if charge2 >= discharge1:
        return True, ""
    
    # Otherwise, they overlap
    return False, "Cycles overlap. Charge and discharge times must not overlap between cycles."


def simulate_battery_operations(
    df: pd.DataFrame,
    capacity_mw: float,
    duration_hours: float,
    efficiency: float,
    charge1: time,
    discharge1: time,
    charge2: Optional[time] = None,
    discharge2: Optional[time] = None,
) -> pd.DataFrame:
    """
    Simulate battery charging and discharging operations.
    
    Args:
        df: DataFrame with datetime and price_eur_per_mwh columns
        capacity_mw: Battery capacity in MW
        duration_hours: Battery duration in hours
        efficiency: Round-trip efficiency (0-1)
        charge1, discharge1: First cycle times
        charge2, discharge2: Optional second cycle times
    
    Returns:
        DataFrame with added columns:
        - charge_mwh: Energy charged in this hour (MWh)
        - discharge_mwh: Energy discharged in this hour (MWh)
        - battery_soc: State of charge at end of hour (MWh)
        - charge_cost: Cost of charging in this hour (€)
        - discharge_revenue: Revenue from discharging in this hour (€)
        - net_revenue: Net revenue (discharge - charge) in this hour (€)
        - cycle: Which cycle this hour belongs to (0 = none, 1 = first, 2 = second)
    """
    df = df.copy()
    df = df.sort_values("datetime_parsed")
    
    capacity_mwh = capacity_mw * duration_hours
    
    # Detect data resolution by checking time differences
    if len(df) > 1:
        time_diffs = df["datetime_parsed"].diff().dropna()
        median_diff = time_diffs.median()
        # If median difference is around 15 minutes, we have 15-minute data
        is_15min_data = median_diff <= pd.Timedelta(minutes=20) and median_diff >= pd.Timedelta(minutes=10)
    else:
        is_15min_data = False
    
    # Calculate interval duration in hours
    if is_15min_data:
        interval_hours = 0.25  # 15 minutes = 0.25 hours
        intervals_per_hour = 4
    else:
        interval_hours = 1.0  # 1 hour
        intervals_per_hour = 1
    
    # Calculate number of intervals for full duration
    total_intervals = int(duration_hours * intervals_per_hour)
    
    # Initialize columns
    df["charge_mwh"] = 0.0
    df["discharge_mwh"] = 0.0
    df["battery_soc"] = 0.0  # State of charge at end of interval
    df["charge_cost"] = 0.0
    df["discharge_revenue"] = 0.0
    df["net_revenue"] = 0.0
    df["cycle"] = 0
    
    # Extract time components
    df["hour_of_day"] = df["datetime_parsed"].dt.hour
    df["minute"] = df["datetime_parsed"].dt.minute
    df["date"] = df["datetime_parsed"].dt.date
    
    # Track cumulative net revenue across ALL days (not reset per day)
    cumulative_net_revenue = 0.0
    
    # Group by date to process each day separately
    for date, day_df in df.groupby("date"):
        day_df = day_df.sort_values("datetime_parsed").copy()
        soc = 0.0  # Start each day with empty battery
        cycle1_charge_cost = 0.0
        cycle2_charge_cost = 0.0
        cycle1_discharge_revenue = 0.0
        cycle2_discharge_revenue = 0.0
        
        # Process all intervals in order to track SOC
        for idx, row in day_df.iterrows():
            dt = row["datetime_parsed"]
            hour = row["hour_of_day"]
            minute = row["minute"]
            is_charging = False
            is_discharging = False
            cycle_num = 0
            
            # Calculate charge/discharge windows based on actual datetime
            # Charge1 window: from charge1 time for duration_hours on this date
            charge1_start_dt = pd.Timestamp(date).replace(hour=charge1.hour, minute=charge1.minute, second=0, microsecond=0)
            charge1_end_dt = charge1_start_dt + pd.Timedelta(hours=duration_hours)
            
            # Discharge1 window: from discharge1 time for duration_hours on this date
            discharge1_start_dt = pd.Timestamp(date).replace(hour=discharge1.hour, minute=discharge1.minute, second=0, microsecond=0)
            discharge1_end_dt = discharge1_start_dt + pd.Timedelta(hours=duration_hours)
            
            # Check if this interval is within charge1 window
            if charge1_start_dt <= dt < charge1_end_dt:
                is_charging = True
                cycle_num = 1
            
            # Check if this interval is within discharge1 window
            if discharge1_start_dt <= dt < discharge1_end_dt:
                is_discharging = True
                cycle_num = 1
            
            # Check cycle 2 if enabled
            if charge2 is not None and discharge2 is not None:
                charge2_start_dt = pd.Timestamp(date).replace(hour=charge2.hour, minute=charge2.minute, second=0, microsecond=0)
                charge2_end_dt = charge2_start_dt + pd.Timedelta(hours=duration_hours)
                
                discharge2_start_dt = pd.Timestamp(date).replace(hour=discharge2.hour, minute=discharge2.minute, second=0, microsecond=0)
                discharge2_end_dt = discharge2_start_dt + pd.Timedelta(hours=duration_hours)
                
                if charge2_start_dt <= dt < charge2_end_dt:
                    is_charging = True
                    cycle_num = 2
                
                if discharge2_start_dt <= dt < discharge2_end_dt:
                    is_discharging = True
                    cycle_num = 2
            
            # Initialize incremental net revenue for this interval
            incremental_net_revenue = 0.0
            
            # Charge if in charging window (and not discharging)
            if is_charging and not is_discharging:
                if soc < capacity_mwh:
                    remaining_capacity = capacity_mwh - soc
                    # Charge at full MW rate for this interval
                    # For hourly data: 10MW * 1.0 hour = 10MWh per hour
                    # For 15-min data: 10MW * 0.25 hour = 2.5MWh per 15-min interval
                    charge_energy = min(capacity_mw * interval_hours, remaining_capacity)
                    if charge_energy > 0:
                        # Cost is negative (money going out)
                        charge_cost_this_interval = -(charge_energy * row["price_eur_per_mwh"])
                        df.loc[idx, "charge_mwh"] = charge_energy
                        df.loc[idx, "charge_cost"] = charge_cost_this_interval  # Already negative
                        df.loc[idx, "cycle"] = cycle_num
                        if cycle_num == 1:
                            cycle1_charge_cost += abs(charge_cost_this_interval)  # Track absolute cost
                        else:
                            cycle2_charge_cost += abs(charge_cost_this_interval)  # Track absolute cost
                        soc = soc + charge_energy
                        # Net revenue for this interval (negative when charging)
                        incremental_net_revenue = charge_cost_this_interval  # Already negative
            
            # Discharge if in discharging window (and not charging)
            elif is_discharging and not is_charging:
                if soc > 0:
                    available_energy = soc * efficiency
                    # Discharge at full MW rate for this interval
                    discharge_energy = min(capacity_mw * interval_hours, available_energy)
                    if discharge_energy > 0:
                        energy_removed_from_battery = discharge_energy / efficiency
                        discharge_revenue_this_interval = discharge_energy * row["price_eur_per_mwh"]
                        df.loc[idx, "discharge_mwh"] = discharge_energy
                        df.loc[idx, "discharge_revenue"] = discharge_revenue_this_interval
                        df.loc[idx, "cycle"] = cycle_num
                        soc = soc - energy_removed_from_battery
                        # Track discharge revenue for this cycle
                        if cycle_num == 1:
                            cycle1_discharge_revenue += discharge_revenue_this_interval
                        else:
                            cycle2_discharge_revenue += discharge_revenue_this_interval
                        # Net revenue for this interval (positive when discharging)
                        incremental_net_revenue = discharge_revenue_this_interval
            
            # Update cumulative net revenue
            cumulative_net_revenue += incremental_net_revenue
            df.loc[idx, "net_revenue"] = cumulative_net_revenue
            
            # Update SOC for all intervals (even when not charging/discharging)
            # SOC is the cumulative MWh in the battery at this point in time
            # It should be calculated as: (cumulative MWh in battery) / (total capacity MWh) * 100
            df.loc[idx, "battery_soc"] = soc
            # Calculate SOC percentage immediately (will be recalculated later with capacity_mwh, but store for debugging)
    
    return df


def compute_bess_metrics(df: pd.DataFrame) -> dict:
    """
    Compute BESS performance metrics.
    
    Returns:
        Dictionary with metrics:
        - total_charge_mwh: Total energy charged (MWh)
        - total_discharge_mwh: Total energy discharged (MWh)
        - avg_charge_price: Average charge price (€/MWh)
        - avg_discharge_price: Average discharge price (€/MWh)
        - avg_spread: Average spread (€/MWh)
        - total_revenue: Total net revenue (€)
        - daily_avg_revenue: Daily average revenue (€)
        - total_cycles: Total number of cycles
    """
    if df.empty:
        return {
            "total_charge_mwh": 0.0,
            "total_discharge_mwh": 0.0,
            "avg_charge_price": 0.0,
            "avg_discharge_price": 0.0,
            "avg_spread": 0.0,
            "total_revenue": 0.0,
            "daily_avg_revenue": 0.0,
            "total_cycles": 0,
        }
    
    # Filter to hours with activity
    active_df = df[(df["charge_mwh"] > 0) | (df["discharge_mwh"] > 0)].copy()
    
    if active_df.empty:
        return {
            "total_charge_mwh": 0.0,
            "total_discharge_mwh": 0.0,
            "avg_charge_price": 0.0,
            "avg_discharge_price": 0.0,
            "avg_spread": 0.0,
            "total_revenue": 0.0,
            "daily_avg_revenue": 0.0,
            "total_cycles": 0,
        }
    
    # Total energy
    total_charge_mwh = active_df["charge_mwh"].sum()
    total_discharge_mwh = active_df["discharge_mwh"].sum()
    
    # Average prices (weighted by energy)
    charge_hours = active_df[active_df["charge_mwh"] > 0]
    discharge_hours = active_df[active_df["discharge_mwh"] > 0]
    
    # Weighted average charge price (total cost / total energy)
    if charge_hours["charge_mwh"].sum() > 0:
        avg_charge_price = charge_hours["charge_cost"].sum() / charge_hours["charge_mwh"].sum()
    else:
        avg_charge_price = 0.0
    
    # Weighted average discharge price (total revenue / total energy)
    if discharge_hours["discharge_mwh"].sum() > 0:
        avg_discharge_price = discharge_hours["discharge_revenue"].sum() / discharge_hours["discharge_mwh"].sum()
    else:
        avg_discharge_price = 0.0
    
    # Average spread (discharge - charge, accounting for efficiency)
    # The spread is the difference in prices, but we need to account for efficiency loss
    # If we charge at price P_c and discharge at price P_d, the effective spread is:
    # P_d - P_c (since we pay P_c per MWh charged, but get P_d per MWh discharged)
    # However, due to efficiency, we discharge less than we charge, so the spread needs adjustment
    avg_spread = avg_discharge_price - avg_charge_price if (avg_charge_price > 0 and avg_discharge_price > 0) else 0.0
    
    # Total revenue (use the last cumulative value from the full dataframe, not just active)
    # net_revenue is already cumulative across all days, so the last value is the total
    if not df.empty:
        # Sort by datetime to ensure we get the last value chronologically
        df_sorted = df.sort_values("datetime_parsed")
        total_revenue = df_sorted["net_revenue"].iloc[-1]
    else:
        total_revenue = 0.0
    
    # Daily average revenue
    num_days = df["date"].nunique()
    daily_avg_revenue = total_revenue / num_days if num_days > 0 else 0.0
    
    # Total cycles (count unique days with at least one discharge)
    total_cycles = active_df[active_df["discharge_mwh"] > 0]["date"].nunique()
    
    return {
        "total_charge_mwh": total_charge_mwh,
        "total_discharge_mwh": total_discharge_mwh,
        "avg_charge_price": avg_charge_price,
        "avg_discharge_price": avg_discharge_price,
        "avg_spread": avg_spread,
        "total_revenue": total_revenue,
        "daily_avg_revenue": daily_avg_revenue,
        "total_cycles": total_cycles,
    }


def main() -> None:
    st.sidebar.header("Data Source")
    
    # Get data source from session state
    source = get_data_source_selector()
    
    # Get inflation rate from session state (only for forecasts)
    inflation_rate = get_inflation_input(source)
    
    # Get date range from session state
    start_dt, end_dt = get_date_range_selector(source)
    
    # Load data with inflation adjustment for forecasts
    df = load_price_data(source, start_dt=start_dt, end_dt=end_dt, inflation_rate=inflation_rate)
    
    if df.empty:
        st.warning(
            "No data found in the database for the selected source and date range."
        )
        return
    
    # Determine title
    if source == "historical_prices":
        title = "BESS Spreads (Historical - ESIOS 600)"
    elif source == "omie_da":
        title = "BESS Spreads (Historical - OMIE DA)"
    else:
        title = f"BESS Spreads (Forecast - {source.capitalize()})"
        if inflation_rate > 0:
            title += f" - {inflation_rate*100:.1f}% Inflation"
    
    st.title(title)
    
    st.markdown("""
    This page calculates battery energy storage system (BESS) spreads and revenues.
    
    **Key features:**
    - Charge battery at specified times
    - Discharge battery at specified times
    - Account for round-trip efficiency losses
    - Calculate average charge/discharge prices and spreads
    - Compute total and daily average revenues (standardized per MWh of capacity)
    """)
    
    # Battery configuration - moved to top of page
    st.subheader("Battery Configuration")
    
    col_config1, col_config2, col_config3, col_config4 = st.columns(4)
    
    with col_config1:
        capacity_mw = st.number_input(
            "Battery Capacity (MW)",
            min_value=0.1,
            max_value=1000.0,
            value=10.0,
            step=0.1,
            format="%.1f",
            key="bess_capacity_mw",
        )
    
    with col_config2:
        duration_hours = st.number_input(
            "Battery Duration (hours)",
            min_value=0.1,
            max_value=24.0,
            value=4.0,
            step=0.1,
            format="%.1f",
            key="bess_duration_hours",
        )
    
    capacity_mwh = capacity_mw * duration_hours
    
    with col_config3:
        st.metric(
            "Total Capacity",
            f"{capacity_mwh:.1f} MWh",
        )
    
    with col_config4:
        efficiency = st.slider(
            "Round-trip Efficiency (%)",
            min_value=50,
            max_value=100,
            value=90,
            step=1,
            key="bess_efficiency",
        ) / 100.0
    
    # Cycle configuration
    st.subheader("Charge/Discharge Times")
    
    col_cycle1, col_cycle2 = st.columns(2)
    
    with col_cycle1:
        num_cycles = st.radio(
            "Number of Cycles per Day",
            options=[1, 2],
            index=0,
            key="bess_num_cycles",
        )
    
    with col_cycle2:
        st.write("")  # Spacer
    
    # Cycle 1
    st.markdown("**Cycle 1:**")
    col_c1_1, col_c1_2 = st.columns(2)
    with col_c1_1:
        st.markdown("**Charge Time:**")
        charge1 = time_input_with_arrows("Charge Time", time(8, 0), "charge1_time")
    with col_c1_2:
        st.markdown("**Discharge Time:**")
        discharge1 = time_input_with_arrows("Discharge Time", time(20, 0), "discharge1_time")
    
    # Cycle 2 (if enabled)
    charge2 = None
    discharge2 = None
    if num_cycles == 2:
        st.markdown("**Cycle 2:**")
        col_c2_1, col_c2_2 = st.columns(2)
        with col_c2_1:
            st.markdown("**Charge Time:**")
            charge2 = time_input_with_arrows("Charge Time", time(12, 0), "charge2_time")
        with col_c2_2:
            st.markdown("**Discharge Time:**")
            discharge2 = time_input_with_arrows("Discharge Time", time(18, 0), "discharge2_time")
    
    # Validate cycles
    is_valid, error_msg = validate_cycles(charge1, discharge1, charge2, discharge2)
    if not is_valid:
        st.error(f"Invalid cycle configuration: {error_msg}")
        return
    
    # Simulate battery operations
    df_with_bess = simulate_battery_operations(
        df,
        capacity_mw=capacity_mw,
        duration_hours=duration_hours,
        efficiency=efficiency,
        charge1=charge1,
        discharge1=discharge1,
        charge2=charge2,
        discharge2=discharge2,
    )
    
    # Compute metrics
    metrics = compute_bess_metrics(df_with_bess)
    
    # Standardize daily revenue per MWh of capacity
    daily_revenue_per_mwh = metrics['daily_avg_revenue'] / capacity_mwh if capacity_mwh > 0 else 0.0
    
    # Display metrics
    st.subheader("BESS Performance Metrics")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            "Average Charge Price",
            f"{metrics['avg_charge_price']:.2f} €/MWh",
        )
    
    with col2:
        st.metric(
            "Average Discharge Price",
            f"{metrics['avg_discharge_price']:.2f} €/MWh",
        )
    
    with col3:
        st.metric(
            "Average Spread",
            f"{metrics['avg_spread']:.2f} €/MWh",
            delta=f"{metrics['avg_spread']:.2f} €/MWh" if metrics['avg_spread'] > 0 else None,
        )
    
    with col4:
        st.metric(
            "Total Cycles",
            f"{metrics['total_cycles']}",
        )
    
    col5, col6 = st.columns(2)
    
    with col5:
        st.metric(
            "Daily Average Revenue",
            f"{metrics['daily_avg_revenue']:.2f} €/day",
        )
    
    with col6:
        st.metric(
            "Daily Revenue per MWh Capacity",
            f"{daily_revenue_per_mwh:.2f} €/MWh/day",
        )
    
    # Calculate aggregations for charge/discharge prices and spreads
    # Filter to intervals with activity
    active_df = df_with_bess[(df_with_bess["charge_mwh"] > 0) | (df_with_bess["discharge_mwh"] > 0)].copy()
    
    if not active_df.empty:
        # Add date components for aggregation
        active_df["date_dt"] = pd.to_datetime(active_df["date"])
        active_df["year"] = active_df["date_dt"].dt.year
        active_df["month"] = active_df["date_dt"].dt.month
        active_df["weekday"] = active_df["date_dt"].dt.day_name()
        active_df["weekday_order"] = active_df["date_dt"].dt.weekday
        
        # Helper function to compute weighted averages for a group
        def compute_price_metrics(group_df):
            charge_df = group_df[group_df["charge_mwh"] > 0]
            discharge_df = group_df[group_df["discharge_mwh"] > 0]
            
            # Weighted average charge price (absolute value since charge_cost is negative)
            if charge_df["charge_mwh"].sum() > 0:
                avg_charge = abs(charge_df["charge_cost"].sum()) / charge_df["charge_mwh"].sum()
            else:
                avg_charge = 0.0
            
            # Weighted average discharge price
            if discharge_df["discharge_mwh"].sum() > 0:
                avg_discharge = discharge_df["discharge_revenue"].sum() / discharge_df["discharge_mwh"].sum()
            else:
                avg_discharge = 0.0
            
            # Average spread
            avg_spread = avg_discharge - avg_charge if (avg_charge > 0 and avg_discharge > 0) else 0.0
            
            return pd.Series({
                "avg_charge_price": avg_charge,
                "avg_discharge_price": avg_discharge,
                "avg_spread": avg_spread,
            })
        
        # Helper to build metric mapping
        def build_metric_mapping(show_charge, show_discharge, show_spread):
            selected = []
            mapping = {}
            if show_charge:
                selected.append("avg_charge_price")
                mapping["avg_charge_price"] = "Charge Price"
            if show_discharge:
                selected.append("avg_discharge_price")
                mapping["avg_discharge_price"] = "Discharge Price"
            if show_spread:
                selected.append("avg_spread")
                mapping["avg_spread"] = "Spread"
            return selected, mapping
        
        # Series visibility toggles for most charts
        st.subheader("Series Visibility")
        col1, col2, col3 = st.columns(3)
        with col1:
            show_charge = st.checkbox("Charge Price", value=True, key="bess_show_charge")
        with col2:
            show_discharge = st.checkbox("Discharge Price", value=True, key="bess_show_discharge")
        with col3:
            show_spread = st.checkbox("Spread", value=True, key="bess_show_spread")
        
        # Filter metrics based on checkboxes
        selected_metrics, metric_mapping = build_metric_mapping(show_charge, show_discharge, show_spread)
        
        if not selected_metrics:
            st.info("Please select at least one series to display.")
            return
        
        # Yearly averages
        st.subheader("Yearly average charge/discharge prices and spreads")
        yearly = active_df.groupby("year").apply(compute_price_metrics).reset_index()
        if not yearly.empty:
            # Create multi-series chart
            yearly_melted = yearly.melt(
                id_vars=["year"],
                value_vars=selected_metrics,
                var_name="metric",
                value_name="price"
            )
            yearly_melted["metric_label"] = yearly_melted["metric"].map(metric_mapping)
            
            chart = create_multi_series_bar_chart(
                yearly_melted,
                x_col="year",
                value_col="price",
                series_col="metric_label",
                x_title="Year",
                y_title="Price (€/MWh)",
                show_labels=True,
            )
            st.altair_chart(chart, use_container_width=True)
        
        # Monthly averages (all months in period) - default to spreads only
        st.subheader("Monthly average charge/discharge prices and spreads (all months in period)")
        # Separate checkboxes for this chart, defaulting to spreads only
        col_m1, col_m2, col_m3 = st.columns(3)
        with col_m1:
            show_charge_m = st.checkbox("Charge Price", value=False, key="bess_show_charge_monthly")
        with col_m2:
            show_discharge_m = st.checkbox("Discharge Price", value=False, key="bess_show_discharge_monthly")
        with col_m3:
            show_spread_m = st.checkbox("Spread", value=True, key="bess_show_spread_monthly")
        
        monthly_metrics, monthly_mapping = build_metric_mapping(show_charge_m, show_discharge_m, show_spread_m)
        if not monthly_metrics:
            monthly_metrics = ["avg_spread"] if "avg_spread" in selected_metrics else selected_metrics
            monthly_mapping = metric_mapping
        
        active_df["year_month"] = active_df["date_dt"].dt.to_period("M").dt.to_timestamp()
        monthly = active_df.groupby("year_month").apply(compute_price_metrics).reset_index()
        if not monthly.empty:
            monthly_melted = monthly.melt(
                id_vars=["year_month"],
                value_vars=monthly_metrics,
                var_name="metric",
                value_name="price"
            )
            monthly_melted["metric_label"] = monthly_melted["metric"].map(monthly_mapping)
            
            chart = create_multi_series_line_chart(
                monthly_melted,
                x_col="year_month",
                value_col="price",
                series_col="metric_label",
                x_title="Month",
                y_title="Price (€/MWh)",
                show_points=False,
                x_format="%b-%y",
            )
            st.altair_chart(chart, use_container_width=True)
        
        # Calendar-month averages (Jan–Dec)
        st.subheader("Calendar-month average charge/discharge prices and spreads (Jan–Dec)")
        month_year = active_df.groupby(["year", "month"]).apply(compute_price_metrics).reset_index()
        if not month_year.empty:
            # Average across all years for each calendar month
            agg_dict = {}
            if "avg_charge_price" in selected_metrics:
                agg_dict["avg_charge_price"] = "mean"
            if "avg_discharge_price" in selected_metrics:
                agg_dict["avg_discharge_price"] = "mean"
            if "avg_spread" in selected_metrics:
                agg_dict["avg_spread"] = "mean"
            
            cal_month = month_year.groupby("month").agg(agg_dict).reset_index()
            cal_month = ensure_all_months(cal_month, "month")
            cal_month["month_name"] = cal_month["month"].apply(
                lambda m: datetime(2000, m, 1).strftime("%b")
            )
            
            cal_month_melted = cal_month.melt(
                id_vars=["month", "month_name"],
                value_vars=[m for m in selected_metrics if m in cal_month.columns],
                var_name="metric",
                value_name="price"
            )
            cal_month_melted["metric_label"] = cal_month_melted["metric"].map(metric_mapping)
            
            chart = create_multi_series_bar_chart(
                cal_month_melted,
                x_col="month_name",
                value_col="price",
                series_col="metric_label",
                x_title="Calendar month",
                y_title="Price (€/MWh)",
                show_labels=True,
                x_sort=MONTH_ORDER,
            )
            st.altair_chart(chart, use_container_width=True)
        
        # Daily averages - default to spreads only, line with no markers
        st.subheader("Daily average charge/discharge prices and spreads")
        # Separate checkboxes for this chart, defaulting to spreads only
        col_d1, col_d2, col_d3 = st.columns(3)
        with col_d1:
            show_charge_d = st.checkbox("Charge Price", value=False, key="bess_show_charge_daily")
        with col_d2:
            show_discharge_d = st.checkbox("Discharge Price", value=False, key="bess_show_discharge_daily")
        with col_d3:
            show_spread_d = st.checkbox("Spread", value=True, key="bess_show_spread_daily")
        
        daily_metrics, daily_mapping = build_metric_mapping(show_charge_d, show_discharge_d, show_spread_d)
        if not daily_metrics:
            daily_metrics = ["avg_spread"] if "avg_spread" in selected_metrics else selected_metrics
            daily_mapping = metric_mapping
        
        daily = active_df.groupby("date").apply(compute_price_metrics).reset_index()
        if not daily.empty:
            daily["date_dt"] = pd.to_datetime(daily["date"])
            daily_melted = daily.melt(
                id_vars=["date", "date_dt"],
                value_vars=daily_metrics,
                var_name="metric",
                value_name="price"
            )
            daily_melted["metric_label"] = daily_melted["metric"].map(daily_mapping)
            
            chart = create_multi_series_line_chart(
                daily_melted,
                x_col="date_dt",
                value_col="price",
                series_col="metric_label",
                x_title="Date",
                y_title="Price (€/MWh)",
                show_points=False,
                x_format="%Y-%m-%d",
            )
            st.altair_chart(chart, use_container_width=True)
        
        # Day-of-week averages
        st.subheader("Day-of-week average charge/discharge prices and spreads")
        dow = active_df.groupby(["weekday", "weekday_order"]).apply(compute_price_metrics).reset_index()
        if not dow.empty:
            dow = ensure_all_days(dow, "weekday", "weekday_order")
            dow_melted = dow.melt(
                id_vars=["weekday", "weekday_order"],
                value_vars=selected_metrics,
                var_name="metric",
                value_name="price"
            )
            dow_melted["metric_label"] = dow_melted["metric"].map(metric_mapping)
            
            chart = create_multi_series_bar_chart(
                dow_melted,
                x_col="weekday",
                value_col="price",
                series_col="metric_label",
                x_title="Day of week",
                y_title="Price (€/MWh)",
                show_labels=True,
                x_sort=DAY_ORDER,
            )
            st.altair_chart(chart, use_container_width=True)
    
    # Raw data
    st.subheader("Raw BESS Operations Data")
    
    # Create a clean display dataframe with consistent datetime formatting
    display_df = df_with_bess.copy()
    
    # Remove duplicates based on datetime_parsed (same timestamp in different formats)
    if "datetime_parsed" in display_df.columns:
        # Keep first occurrence if there are duplicates based on parsed datetime
        display_df = display_df.drop_duplicates(subset=["datetime_parsed"], keep="first")
        display_df = display_df.sort_values("datetime_parsed")
        
        # Use datetime_parsed formatted consistently for display
        display_df["datetime"] = display_df["datetime_parsed"].dt.strftime("%Y-%m-%d %H:%M:%S")
    
    cols = [
        "datetime",
        "price_eur_per_mwh",
        "charge_mwh",
        "discharge_mwh",
        "battery_soc",
        "charge_cost",
        "discharge_revenue",
        "net_revenue",
        "cycle",
    ]
    # Add state of charge percentage if battery_soc exists
    if "battery_soc" in display_df.columns and capacity_mwh > 0:
        display_df["soc_percent"] = (display_df["battery_soc"] / capacity_mwh * 100).round(1)
        # Ensure SOC% is between 0 and 100
        display_df["soc_percent"] = display_df["soc_percent"].clip(0, 100)
        cols.insert(cols.index("battery_soc") + 1, "soc_percent")
    
    available_cols = [c for c in cols if c in display_df.columns]
    st.dataframe(display_df[available_cols], use_container_width=True, height=400)
    
    # Format datetime for CSV export
    from utils import format_datetime_for_csv
    df_export = format_datetime_for_csv(display_df[available_cols])
    csv = df_export.to_csv(index=False).encode("utf-8")
    start_date_str = start_dt.strftime("%Y-%m-%d")
    end_date_str = end_dt.strftime("%Y-%m-%d")
    st.download_button(
        label="Download BESS data as CSV",
        data=csv,
        file_name=f"bess_spreads_{source}_{start_date_str}_{end_date_str}.csv",
        mime="text/csv",
    )


# Streamlit automatically calls this when the page is loaded
main()

