import sqlite3
from datetime import datetime, time, timedelta

import altair as alt
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import streamlit as st

st.set_page_config(
    page_title="Prices Distribution",
    page_icon=":bar_chart:",
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
from chart_config import BRAND_COLOR, MONTH_ORDER, get_chart_title


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
    st.title("Prices Distribution")
    
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
    if source not in ("historical_prices", "omie_da") and inflation_rate > 0:
        st.markdown(f"**Inflation:** inflated at {inflation_rate*100:.1f}% p.a.")
    
    st.markdown("""
    This page shows the distribution of electricity prices and analyzes how many hours fall at or below a specified price threshold.
    Use the threshold slider to explore price patterns and identify periods of low prices.
    """)
    
    df_filtered = df.copy()

    # Standardise to hourly resolution: average of all readings within each hour.
    # First build a global hourly table so we can compute overall percentages.
    df_all = df_filtered.copy()
    df_all["hour_key"] = df_all["datetime_parsed"].dt.floor("H")
    hourly_all = (
        df_all.groupby("hour_key", as_index=False)
        .agg(
            price_eur_per_mwh=("price_eur_per_mwh", "mean"),
        )
        .rename(columns={"hour_key": "datetime_hour"})
    )
    total_hours_all = len(hourly_all)

    # Hourly for the selected window
    df_filtered["hour_key"] = df_filtered["datetime_parsed"].dt.floor("H")
    hourly_base = (
        df_filtered.groupby("hour_key", as_index=False)
        .agg(
            price_eur_per_mwh=("price_eur_per_mwh", "mean"),
            year=("year", "first"),
            month=("month", "first"),
            day=("day", "first"),
            hour=("hour", "first"),
        )
        .rename(columns={"hour_key": "datetime_hour"})
    )

    total_hours_sel = len(hourly_base)

    st.subheader("Price distribution (histogram)")
    if hourly_base.empty:
        st.info("No data in the selected window.")
        return
    
    # Get threshold first (needed for bin alignment)
    threshold = st.slider(
        "Threshold price (€/MWh)",
        min_value=-10.0,
        max_value=50.0,
        value=0.0,
        step=0.25,
    )
    
    # Allow shrinking the histogram x-axis range
    price_min = float(hourly_base["price_eur_per_mwh"].min())
    price_max = float(hourly_base["price_eur_per_mwh"].max())

    # Use session state to persist slider value across reruns
    # Key includes source so each data source has its own slider state
    slider_key = f"price_range_slider_{source}"
    
    # Get current slider value from session state, or use defaults if not set
    if slider_key not in st.session_state:
        # First time for this source, use full range
        default_value = (price_min, price_max)
    else:
        # Get stored value and validate it's within current bounds
        stored_min, stored_max = st.session_state[slider_key]
        # Clamp to current bounds if data range changed
        stored_min = max(price_min, min(stored_min, price_max))
        stored_max = min(price_max, max(stored_max, price_min))
        # Ensure min <= max
        if stored_min > stored_max:
            default_value = (price_min, price_max)
        else:
            default_value = (stored_min, stored_max)
    
    x_min, x_max = st.slider(
        "Price range for histogram (€/MWh)",
        min_value=price_min,
        max_value=price_max,
        value=default_value,
        step=0.5,
        key=slider_key,
    )

    bin_width = st.slider(
        "Histogram bin width (€/MWh)",
        min_value=0.5,
        max_value=max(1.0, (price_max - price_min) / 10),
        value=1.0,
        step=0.5,
    )

    # Align bins so that threshold is the upper bound (inclusive) of one bin
    # With numpy histogram right=True, bins are (left, right], so threshold at edges[i+1] 
    # means values <= threshold go into bin i (inclusive upper bound)
    # We want: threshold = bin_start + n * bin_width for some integer n
    # So: bin_start = threshold - n * bin_width
    # Find n such that bin_start <= x_min (to cover the range)
    n_bins_before_threshold = int(np.ceil((threshold - x_min) / bin_width))
    bin_start = threshold - n_bins_before_threshold * bin_width
    
    # Ensure bin_start is at or below x_min to cover the full range
    if bin_start > x_min:
        bin_start = threshold - (n_bins_before_threshold + 1) * bin_width
    
    # Create bins starting from bin_start, ensuring threshold is exactly at upper bound of one bin
    # Calculate how many bins we need to cover [bin_start, x_max]
    n_bins_total = int(np.ceil((x_max - bin_start) / bin_width)) + 1
    bins = np.arange(bin_start, bin_start + (n_bins_total + 1) * bin_width + bin_width * 0.01, bin_width)
    
    # Find which bin edge is closest to threshold and set it exactly to threshold
    threshold_bin_idx = None
    closest_edge_idx = np.argmin(np.abs(bins - threshold))
    bins[closest_edge_idx] = threshold
    
    # Determine which bin has threshold as its upper bound
    # With right=True, bin i is (edges[i], edges[i+1]], so if edges[i+1] == threshold,
    # then bin i has threshold as inclusive upper bound
    if closest_edge_idx > 0 and closest_edge_idx < len(bins):
        threshold_bin_idx = closest_edge_idx - 1
    
    # Build histogram where values outside [x_min, x_max] are clipped into
    # the first/last bin so total hours are conserved.
    prices = np.clip(
        hourly_base["price_eur_per_mwh"].to_numpy(dtype=float), x_min, x_max
    )
    
    # Use np.histogram with right parameter if available, otherwise use workaround
    # right=True makes bins (left, right] - inclusive on the right
    try:
        # Try using right=True (available in NumPy >= 1.11.0)
        counts, edges = np.histogram(prices, bins=bins, right=True)
    except TypeError:
        # Fallback for older NumPy versions: use np.digitize for right-inclusive bins
        # np.digitize with right=True gives us (left, right] behavior
        try:
            # Try np.digitize with right parameter
            bin_indices = np.digitize(prices, bins, right=True)
            # Count values in each bin (bin_indices are 1-indexed, with 0 for values < bins[0])
            counts = np.bincount(bin_indices, minlength=len(bins))
            # Remove the count for values below first bin (index 0)
            if len(counts) > len(bins):
                counts = counts[1:]
            # Ensure counts has correct length
            counts = counts[:len(bins)-1]
            edges = bins
        except TypeError:
            # If np.digitize also doesn't support right, use manual binning
            # Default histogram gives [left, right), we want (left, right]
            counts, edges = np.histogram(prices, bins=bins)
            # Manually adjust: for each bin edge, move values exactly at edge to previous bin
            for i in range(1, len(edges)):
                at_edge = np.sum(prices == edges[i])
                if at_edge > 0 and i-1 < len(counts):
                    # Values at edges[i] should go to bin i-1 (right-inclusive)
                    if i < len(counts):
                        counts[i] -= at_edge
                    counts[i-1] += at_edge
    if total_hours_sel > 0:
        perc = counts / total_hours_sel * 100.0
    else:
        perc = np.zeros_like(counts, dtype=float)

    bin_centers = (edges[:-1] + edges[1:]) / 2.0

    fig_hist = make_subplots(specs=[[{"secondary_y": True}]])
    # Bars for hours (left axis)
    fig_hist.add_bar(
        x=bin_centers,
        y=counts,
        name="Hours",
        marker_color=BRAND_COLOR,
        secondary_y=False,
        customdata=np.stack(
            [edges[:-1], edges[1:], np.round(perc).astype(int), np.arange(len(edges)-1)], axis=-1
        ),
        hovertemplate=(
            "<b>Bin: %{customdata[0]:.2f}–%{customdata[1]:.2f} €/MWh</b>"
            "<br>Boundaries: (%{customdata[0]:.2f}, %{customdata[1]:.2f}]"
            "<br>Hours: %{y}"
            "<br>Percent: %{customdata[2]}%<extra></extra>"
        ),
    )
    # Transparent line for percent (right axis) - just to activate the axis
    fig_hist.add_trace(
        go.Scatter(
            x=bin_centers,
            y=perc,
            mode="lines",
            name="Percent",
            line=dict(color="rgba(0,0,0,0)", width=0),  # Fully transparent
            hovertemplate=(
                "<b>Bin: %{customdata[0]:.2f}–%{customdata[1]:.2f} €/MWh</b>"
                "<br>Boundaries: (%{customdata[0]:.2f}, %{customdata[1]:.2f}]"
                "<br>Hours: %{customdata[2]}"
                "<br>Percent: %{y:.0f}%<extra></extra>"
            ),
            customdata=np.stack(
                [edges[:-1], edges[1:], counts], axis=-1
            ),
            showlegend=False,
        ),
        secondary_y=True,
    )

    # Calculate max percentage to scale the right axis appropriately
    max_percent = float(np.max(perc)) if len(perc) > 0 else 100.0
    # Cap at 100% but allow it to scale down if max is lower
    pct_axis_max = min(max(max_percent * 1.1, 10.0), 100.0)  # Add 10% padding, but cap at 100%
    
    fig_hist.update_xaxes(title_text="Price (€/MWh)")
    fig_hist.update_yaxes(
        title_text="Hours",
        secondary_y=False,
        range=[0, None],  # Start from 0 to ensure bars render from baseline
    )
    fig_hist.update_yaxes(
        title_text="Percent of hours",
        secondary_y=True,
        range=[0, pct_axis_max],  # Scale to data, capped at 100%
        ticksuffix="%",
        showgrid=False,
    )
    fig_hist.update_layout(
        showlegend=False,
        bargap=0,
        bargroupgap=0,
        margin=dict(l=60, r=60, t=10, b=50),
        height=300,
    )

    st.plotly_chart(fig_hist, use_container_width=True)

    st.subheader("Hours at or below price threshold")

    th_df = hourly_base[hourly_base["price_eur_per_mwh"] <= threshold].copy()
    total_hours = len(th_df)
    
    # Calculate percentage of hours at or below threshold
    if total_hours_sel > 0:
        pct_threshold = total_hours / total_hours_sel * 100
    else:
        pct_threshold = 0.0
    
    start_date_str = start_dt.strftime("%Y-%m-%d")
    end_date_str = end_dt.strftime("%Y-%m-%d")
    st.markdown(
        (
            f"<h3 style='font-weight:700;'>"
            f"Total hours at or below {threshold:.2f} €/MWh "
            f"({start_date_str} – {end_date_str}): "
            f"{total_hours} ({pct_threshold:.0f}% of hours in selection)"
            f"</h3>"
        ),
        unsafe_allow_html=True,
    )

    if total_hours == 0:
        st.info("No hours at or below the selected threshold in this window.")
        return

    # Yearly counts
    st.subheader(get_chart_title("yearly", "prices"))
    yearly_totals = (
        hourly_base.groupby("year", as_index=False)["price_eur_per_mwh"].count()
    ).rename(columns={"price_eur_per_mwh": "total_hours"})
    yearly_hits = (
        th_df.groupby("year", as_index=False)["price_eur_per_mwh"].count()
    ).rename(columns={"price_eur_per_mwh": "hours"})
    yearly = yearly_hits.merge(yearly_totals, on="year", how="left")
    yearly["percent"] = yearly["hours"] / yearly["total_hours"] * 100
    # Format hours label with thousand separator
    yearly["hours_label"] = yearly["hours"].apply(lambda x: f"{int(x):,}")
    # Format percent for tooltip with 1 decimal and % sign
    yearly["percent_label"] = yearly["percent"].apply(lambda x: f"{x:.1f}%")

    y_title_hours = f"# hours <= {threshold:.2f}"

    yearly_hours_chart = (
        alt.Chart(yearly)
        .mark_bar(color=BRAND_COLOR)
        .encode(
            x=alt.X("year:O", title="Year"),
            y=alt.Y(
                "hours:Q",
                title=y_title_hours
            ),
            tooltip=[
                alt.Tooltip("year:O", title="Year"),
                alt.Tooltip("hours:Q", title="Hours", format=",.0f"),
                alt.Tooltip("percent_label:N", title="Percent"),
            ],
        )
        .properties(height=250)
    )

    yearly_text = (
        alt.Chart(yearly)
        .mark_text(baseline="bottom", dy=-5)
        .encode(
            x="year:O",
            y=alt.Y("hours:Q",axis=None),
            text="hours_label:N",
        )
    )

    yearly_pct_axis = (
        alt.Chart(yearly)
        .mark_line(color="transparent")
        .encode(
            x="year:O",
            y=alt.Y(
                "percent:Q",
                scale=alt.Scale(nice=True),
                axis=alt.Axis(title="Percent of hours", orient="right", format=".0f")
            ),
        )
    )

    st.altair_chart(
        alt.layer(yearly_hours_chart, yearly_text, yearly_pct_axis)
        .resolve_scale(y="independent"),
        use_container_width=True,
    )

    # Monthly counts – all months in window
    st.subheader(get_chart_title("year_month", "prices"))
    monthly = th_df.copy()
    monthly["year_month"] = (
        monthly["datetime_hour"].dt.to_period("M").dt.to_timestamp()
    )
    monthly_agg = monthly.groupby("year_month", as_index=False)[
        "price_eur_per_mwh"
    ].count()
    monthly_agg = monthly_agg.rename(columns={"price_eur_per_mwh": "hours"})
    
    # Get total hours per month from hourly_base
    monthly_totals = hourly_base.copy()
    monthly_totals["year_month"] = (
        monthly_totals["datetime_hour"].dt.to_period("M").dt.to_timestamp()
    )
    monthly_totals_agg = monthly_totals.groupby("year_month", as_index=False)[
        "price_eur_per_mwh"
    ].count().rename(columns={"price_eur_per_mwh": "total_hours"})
    
    monthly_agg = monthly_agg.merge(monthly_totals_agg, on="year_month", how="left")
    monthly_agg["total_hours"] = monthly_agg["total_hours"].fillna(0)
    # Calculate percent as hours in that month / total hours in that month
    monthly_agg["percent"] = monthly_agg.apply(
        lambda row: (row["hours"] / row["total_hours"] * 100) if row["total_hours"] > 0 else 0,
        axis=1,
    )
    # Format hours label with thousand separator (for data labels if needed)
    monthly_agg["hours_label"] = monthly_agg["hours"].apply(lambda x: f"{int(x):,}")
    # Format percent for tooltip with 1 decimal and % sign
    monthly_agg["percent_label"] = monthly_agg["percent"].apply(lambda x: f"{x:.1f}%")

    monthly_base = (
        alt.Chart(monthly_agg)
        .mark_bar(color=BRAND_COLOR)
        .encode(
            x=alt.X(
                "year_month:T",
                title="Month",
                axis=alt.Axis(format="%b-%y", labelAngle=-45),
            ),
            y=alt.Y("hours:Q", title=f"# hours <= {threshold:.2f}"),
            tooltip=[
                alt.Tooltip("year_month:T", title="Month", format="%Y-%m"),
                alt.Tooltip("hours:Q", title="Hours", format=",.0f"),
                alt.Tooltip("percent_label:N", title="Percent"),
            ],
        )
        .properties(height=250)
    )
    monthly_pct_axis = (
        alt.Chart(monthly_agg)
        .mark_line(color="transparent")
        .encode(
            x="year_month:T",
            y=alt.Y(
                "percent:Q",
                scale=alt.Scale(nice=True),
                axis=alt.Axis(
                    title="Percent of hours", orient="right", format=".0f"
                ),
            ),
        )
    )
    st.altair_chart(
        alt.layer(monthly_base, monthly_pct_axis).resolve_scale(y="independent"),
        use_container_width=True,
    )

    # Monthly counts – average calendar month (Jan–Dec) over years
    st.subheader(get_chart_title("calendar_month", "prices"))
    monthly_by_year = th_df.copy()
    monthly_by_year["year"] = monthly_by_year["datetime_hour"].dt.year
    monthly_by_year["month"] = monthly_by_year["datetime_hour"].dt.month
    # hours per (year, month) at or below threshold
    ym_counts = (
        monthly_by_year.groupby(["year", "month"], as_index=False)["price_eur_per_mwh"]
        .count()
        .rename(columns={"price_eur_per_mwh": "hours"})
    )
    
    # Get total hours per (year, month) from hourly_base
    monthly_totals_by_year = hourly_base.copy()
    monthly_totals_by_year["year"] = monthly_totals_by_year["datetime_hour"].dt.year
    monthly_totals_by_year["month"] = monthly_totals_by_year["datetime_hour"].dt.month
    ym_totals = (
        monthly_totals_by_year.groupby(["year", "month"], as_index=False)["price_eur_per_mwh"]
        .count()
        .rename(columns={"price_eur_per_mwh": "total_hours"})
    )
    
    # Merge to get percentages per (year, month)
    ym_merged = ym_counts.merge(ym_totals, on=["year", "month"], how="left")
    ym_merged["total_hours"] = ym_merged["total_hours"].fillna(0)
    ym_merged["percent"] = ym_merged.apply(
        lambda row: (row["hours"] / row["total_hours"] * 100) if row["total_hours"] > 0 else 0,
        axis=1,
    )
    
    # Average over years for each calendar month
    month_avg = (
        ym_merged.groupby("month", as_index=False).agg(
            hours_avg=("hours", "mean"),
            percent_avg=("percent", "mean"),
        )
    )
    # Ensure all 12 calendar months are present, in order
    all_months = pd.DataFrame({"month": list(range(1, 13))})
    month_avg = all_months.merge(month_avg, on="month", how="left").fillna(0)
    month_avg["month_name"] = month_avg["month"].apply(
        lambda m: datetime(2000, m, 1).strftime("%b")
    )
    month_title = f"# hours <= {threshold:.2f}"
    # Format label with just hours count (no "h" and no %), with thousand separator
    month_avg["label"] = month_avg.apply(
        lambda r: f"{int(round(r['hours_avg'])):,}",
        axis=1,
    )
    # Format percent for tooltip with 1 decimal and % sign
    month_avg["percent_label"] = month_avg["percent_avg"].apply(lambda x: f"{x:.1f}%")

    month_chart = (
        alt.Chart(month_avg)
        .mark_bar(color=BRAND_COLOR)
        .encode(
            x=alt.X(
                "month_name:O",
                title="Calendar month",
                sort=MONTH_ORDER,
            ),
            y=alt.Y(
                "hours_avg:Q",
                title=month_title
            ),
            tooltip=[
                alt.Tooltip("month_name:O", title="Month"),
                alt.Tooltip("hours_avg:Q", title="Hours", format=",.0f"),
                alt.Tooltip("percent_label:N", title="Percent"),
            ],
        )
        .properties(height=250)
    )

    month_text = (
        alt.Chart(month_avg)
        .mark_text(baseline="bottom", dy=-5)
        .encode(
            x=alt.X(
                "month_name:O",
                sort=MONTH_ORDER
            ),
            y=alt.Y("hours_avg:Q",axis=None),
            text="label:N",
        )
    )

    month_pct_axis = (
        alt.Chart(month_avg)
        .mark_line(color="transparent")
        .encode(
            x=alt.X("month_name:O", sort=MONTH_ORDER),
            y=alt.Y(
                "percent_avg:Q",
                scale=alt.Scale(nice=True),
                axis=alt.Axis(title="Percent of hours", orient="right", format=".0f")
            ),
        )
    )

    st.altair_chart(
        alt.layer(month_chart, month_text, month_pct_axis)
        .resolve_scale(y="independent"),
        use_container_width=True,
    )

    # Daily counts
    st.subheader(get_chart_title("daily", "prices"))
    daily = th_df.copy()
    daily["date"] = daily["datetime_hour"].dt.date
    daily_agg = daily.groupby("date", as_index=False)["price_eur_per_mwh"].count()
    daily_agg = daily_agg.rename(columns={"price_eur_per_mwh": "hours"})
    
    # Get total hours per day from hourly_base
    daily_totals = hourly_base.copy()
    daily_totals["date"] = daily_totals["datetime_hour"].dt.date
    daily_totals_agg = daily_totals.groupby("date", as_index=False)[
        "price_eur_per_mwh"
    ].count().rename(columns={"price_eur_per_mwh": "total_hours"})
    
    daily_agg = daily_agg.merge(daily_totals_agg, on="date", how="left")
    daily_agg["total_hours"] = daily_agg["total_hours"].fillna(0)
    # Calculate percent as hours in that day / total hours in that day
    daily_agg["percent"] = daily_agg.apply(
        lambda row: (row["hours"] / row["total_hours"] * 100) if row["total_hours"] > 0 else 0,
        axis=1,
    )
    # Format hours label with thousand separator (for data labels if needed)
    daily_agg["hours_label"] = daily_agg["hours"].apply(lambda x: f"{int(x):,}")
    # Format percent for tooltip with 1 decimal and % sign
    daily_agg["percent_label"] = daily_agg["percent"].apply(lambda x: f"{x:.1f}%")

    daily_base = (
        alt.Chart(daily_agg)
        .mark_bar(color=BRAND_COLOR)
        .encode(
            x=alt.X(
                "date:T",
                title="Date",
                axis=alt.Axis(format="%b-%y", labelAngle=-45),
            ),
            y=alt.Y("hours:Q", title=f"# hours <= {threshold:.2f}"),
            tooltip=[
                alt.Tooltip("date:T", title="Date", format="%Y-%m-%d"),
                alt.Tooltip("hours:Q", title="Hours", format=",.0f"),
                alt.Tooltip("percent_label:N", title="Percent"),
            ],
        )
        .properties(height=250)
    )
    daily_pct_axis = (
        alt.Chart(daily_agg)
        .mark_line(color="transparent")
        .encode(
            x="date:T",
            y=alt.Y(
                "percent:Q",
                scale=alt.Scale(nice=True),
                axis=alt.Axis(
                    title="Percent of hours", orient="right", format=".1f"
                ),
            ),
        )
    )
    st.altair_chart(
        alt.layer(daily_base, daily_pct_axis).resolve_scale(y="independent"),
        use_container_width=True,
    )

    # Day-of-week counts
    st.subheader(get_chart_title("day_of_week", "prices"))
    dow = th_df.copy()
    dow["weekday"] = dow["datetime_hour"].dt.day_name()
    dow["weekday_order"] = dow["datetime_hour"].dt.weekday  # Monday=0, Sunday=6
    dow_agg = dow.groupby(["weekday", "weekday_order"], as_index=False)[
        "price_eur_per_mwh"
    ].count()
    dow_agg = dow_agg.rename(columns={"price_eur_per_mwh": "hours"})
    
    # Get total hours per weekday from hourly_base
    dow_totals = hourly_base.copy()
    dow_totals["weekday"] = dow_totals["datetime_hour"].dt.day_name()
    dow_totals["weekday_order"] = dow_totals["datetime_hour"].dt.weekday
    dow_totals_agg = dow_totals.groupby(["weekday", "weekday_order"], as_index=False)[
        "price_eur_per_mwh"
    ].count().rename(columns={"price_eur_per_mwh": "total_hours"})
    
    dow_agg = dow_agg.merge(dow_totals_agg, on=["weekday", "weekday_order"], how="left")
    dow_agg["total_hours"] = dow_agg["total_hours"].fillna(0)
    # Calculate percent as hours in that weekday / total hours in that weekday
    dow_agg["percent"] = dow_agg.apply(
        lambda row: (row["hours"] / row["total_hours"] * 100) if row["total_hours"] > 0 else 0,
        axis=1,
    )
    
    # Ensure all 7 weekdays are present, in order
    order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    all_days = pd.DataFrame({
        "weekday": order,
        "weekday_order": list(range(7))
    })
    dow_agg = all_days.merge(dow_agg, on=["weekday", "weekday_order"], how="left")
    dow_agg[["hours", "total_hours", "percent"]] = dow_agg[["hours", "total_hours", "percent"]].fillna(0)
    dow_agg = dow_agg.sort_values("weekday_order")
    # Format hours label with thousand separator
    dow_agg["hours_label"] = dow_agg["hours"].apply(lambda x: f"{int(x):,}")
    # Format percent for tooltip with 1 decimal and % sign
    dow_agg["percent_label"] = dow_agg["percent"].apply(lambda x: f"{x:.1f}%")
    
    y_title_dow = f"# hours <= {threshold:.2f}"
    
    dow_hours_chart = (
        alt.Chart(dow_agg)
        .mark_bar(color=BRAND_COLOR)
        .encode(
            x=alt.X(
                "weekday:O",
                title="Day of week",
                sort=order,
            ),
            y=alt.Y(
                "hours:Q",
                title=y_title_dow
            ),
            tooltip=[
                alt.Tooltip("weekday:O", title="Day"),
                alt.Tooltip("hours:Q", title="Hours", format=",.0f"),
                alt.Tooltip("percent_label:N", title="Percent"),
            ],
        )
        .properties(height=250)
    )
    
    dow_text = (
        alt.Chart(dow_agg)
        .mark_text(baseline="bottom", dy=-5)
        .encode(
            x=alt.X("weekday:O", sort=order),
            y=alt.Y("hours:Q", axis=None),
            text="hours_label:N",
        )
    )
    
    dow_pct_axis = (
        alt.Chart(dow_agg)
        .mark_line(color="transparent")
        .encode(
            x=alt.X("weekday:O", sort=order),
            y=alt.Y(
                "percent:Q",
                scale=alt.Scale(nice=True),
                axis=alt.Axis(title="Percent of hours", orient="right", format=".1f")
            ),
        )
    )
    
    st.altair_chart(
        alt.layer(dow_hours_chart, dow_text, dow_pct_axis)
        .resolve_scale(y="independent"),
        use_container_width=True,
    )

    # Hour-of-day counts
    st.subheader(get_chart_title("hour_of_day", "prices"))
    hourly_totals = (
        hourly_base.groupby("hour", as_index=False)["price_eur_per_mwh"].count()
    ).rename(columns={"price_eur_per_mwh": "total_hours"})
    hourly_hits = (
        th_df.groupby("hour", as_index=False)["price_eur_per_mwh"].count()
    ).rename(columns={"price_eur_per_mwh": "hours"})

    # Ensure all 24 hours 0–23 exist
    all_hours = pd.DataFrame({"hour": list(range(24))})
    hourly = all_hours.merge(hourly_totals, on="hour", how="left").merge(
        hourly_hits, on="hour", how="left"
    )
    hourly[["total_hours", "hours"]] = hourly[["total_hours", "hours"]].fillna(0)
    # Avoid division by zero
    hourly["percent"] = hourly.apply(
        lambda row: (row["hours"] / row["total_hours"] * 100) if row["total_hours"] > 0 else 0,
        axis=1,
    )
    # Format hours label with thousand separator
    hourly["hours_label"] = hourly["hours"].apply(lambda x: f"{int(x):,}")
    # Format percent for tooltip with 1 decimal and % sign
    hourly["percent_label"] = hourly["percent"].apply(lambda x: f"{x:.1f}%")

    y_title_h_hours = f"# hours <= {threshold:.2f}"

    hourly_hours_chart = (
        alt.Chart(hourly)
        .mark_bar(color=BRAND_COLOR)
        .encode(
            x=alt.X("hour:O", title="Hour of day"),
            y=alt.Y(
                "hours:Q",
                title=y_title_h_hours
            ),
            tooltip=[
                alt.Tooltip("hour:O", title="Hour"),
                alt.Tooltip("hours:Q", title="Hours", format=",.0f"),
                alt.Tooltip("percent_label:N", title="Percent"),
            ],
        )
        .properties(height=250)
    )

    hourly_text = (
        alt.Chart(hourly)
        .mark_text(baseline="bottom", dy=-5)
        .encode(
            x="hour:O",
            y=alt.Y("hours:Q", axis=None),
            text="hours_label:N",
        )
    )

    hourly_pct_axis = (
        alt.Chart(hourly)
        .mark_line(color="transparent")
        .encode(
            x="hour:O",
            y=alt.Y(
                "percent:Q",
                scale=alt.Scale(nice=True),
                axis=alt.Axis(title="Percent of hours", orient="right", format=".0f")
            ),
        )
    )

    st.altair_chart(
        alt.layer(hourly_hours_chart, hourly_text, hourly_pct_axis)
        .resolve_scale(y="independent"),
        use_container_width=True,
    )

# Streamlit automatically calls this when the page is loaded
main()


