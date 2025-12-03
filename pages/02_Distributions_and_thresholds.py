import sqlite3
from datetime import datetime, time, timedelta

import altair as alt
import pandas as pd
import streamlit as st

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

    parsed = df["datetime"].apply(_parse_timestamp)
    df = df[parsed.notna()].copy()
    df["datetime_parsed"] = parsed[parsed.notna()]
    return df


def main() -> None:
    st.sidebar.header("Price Distribution")
    indicator_id = st.sidebar.radio(
        "Indicator",
        options=list(INDICATORS.keys()),
        index=0,
        format_func=lambda k: f"{k} – {INDICATORS.get(k, str(k))}",
    )

    st.title(f"Price Distribution (ESIOS {indicator_id} – {INDICATORS.get(indicator_id, '')})")

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
    default_start_date = max(max_date - timedelta(days=365), min_date)

    st.sidebar.header("Filter")
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

    # Standardise to hourly resolution: average of all readings within each hour.
    # First build a global hourly table so we can compute overall percentages.
    df_all = df.copy()
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
    if total_hours_all > 0:
        pct_sel = total_hours_sel / total_hours_all * 100
    else:
        pct_sel = 0.0

    st.markdown(
        f"**Total hours in selection: {total_hours_sel} ({pct_sel:.0f}% of all hours)**"
    )

    st.subheader("Price distribution (histogram)")
    if hourly_base.empty:
        st.info("No data in the selected window.")
        return
    # Allow shrinking the histogram x-axis range
    price_min = float(hourly_base["price_eur_per_mwh"].min())
    price_max = float(hourly_base["price_eur_per_mwh"].max())

    x_min, x_max = st.slider(
        "Price range for histogram (€/MWh)",
        min_value=price_min,
        max_value=price_max,
        value=(price_min, price_max),
        step=0.5,
    )

    bin_width = st.slider(
        "Histogram bin width (€/MWh)",
        min_value=0.5,
        max_value=max(1.0, (price_max - price_min) / 10),
        value=1.0,
        step=0.5,
    )

    # Build histogram where values outside [x_min, x_max] are clipped into
    # the first/last bin so total hours are conserved.
    hist_chart = (
        alt.Chart(hourly_base)
        .transform_calculate(
            price_clipped=f"clamp(datum.price_eur_per_mwh, {x_min}, {x_max})"
        )
        # First positional arg is the output field(s), second is the source field.
        # Here we ask Altair/Vega-Lite to produce both bin_start and bin_end.
        .transform_bin(
            ["bin_start", "bin_end"],
            "price_clipped",
            bin=alt.Bin(extent=[x_min, x_max], step=bin_width),
        )
        .transform_aggregate(
            count="count()",
            groupby=["bin_start", "bin_end"],
        )
        .mark_bar()
        .encode(
            x=alt.X(
                "bin_start:Q",
                title="Price (€/MWh)",
            ),
            x2="bin_end:Q",
            y=alt.Y("count:Q", title="Number of hours"),
            tooltip=[
                alt.Tooltip("bin_start:Q", title="From (€/MWh)", format=".2f"),
                alt.Tooltip("bin_end:Q", title="To (€/MWh)", format=".2f"),
                alt.Tooltip("count:Q", title="Hours"),
            ],
        )
        .properties(height=300)
    )
    st.altair_chart(hist_chart, use_container_width=True)

    st.subheader("Hours at or below price threshold")

    threshold = st.slider(
        "Threshold price (€/MWh)",
        min_value=-10.0,
        max_value=50.0,
        value=0.0,
        step=0.25,
    )

    th_df = hourly_base[hourly_base["price_eur_per_mwh"] <= threshold].copy()
    total_hours = len(th_df)
    st.markdown(
        f"**Total hours at or below {threshold:.2f} €/MWh in this window: {total_hours}**"
    )

    if total_hours == 0:
        st.info("No hours at or below the selected threshold in this window.")
        return

    # Yearly counts
    st.markdown("**By year**")
    yearly_totals = (
        hourly_base.groupby("year", as_index=False)["price_eur_per_mwh"].count()
    ).rename(columns={"price_eur_per_mwh": "total_hours"})
    yearly_hits = (
        th_df.groupby("year", as_index=False)["price_eur_per_mwh"].count()
    ).rename(columns={"price_eur_per_mwh": "hours"})
    yearly = yearly_hits.merge(yearly_totals, on="year", how="left")
    yearly["percent"] = yearly["hours"] / yearly["total_hours"] * 100

    y_title_hours = "Hours ≤ threshold"
    y_title_pct = "Hours ≤ threshold (% of hours in year)"

    yearly_hours_chart = (
        alt.Chart(yearly)
        .mark_bar()
        .encode(
            x=alt.X("year:O", title="Year"),
            y=alt.Y("hours:Q", title=y_title_hours),
            tooltip=[
                alt.Tooltip("year:O", title="Year"),
                alt.Tooltip("hours:Q", title="Hours"),
                alt.Tooltip("percent:Q", title="Percent", format=".0f"),
            ],
        )
        .properties(height=250)
    )
    yearly_text = (
        alt.Chart(yearly)
        .mark_text(baseline="bottom", dy=-5)
        .encode(
            x="year:O",
            y="hours:Q",
            text=alt.Text("hours:Q", format=".0f"),
        )
    )
    yearly_pct_chart = (
        alt.Chart(yearly)
        .mark_line(color="red")
        .encode(
            x=alt.X("year:O", title="Year"),
            y=alt.Y(
                "percent:Q",
                title=y_title_pct,
                axis=alt.Axis(titleColor="red", orient="right", format=".0f"),
            ),
        )
    )
    st.altair_chart(
        alt.layer(yearly_hours_chart, yearly_text, yearly_pct_chart).resolve_scale(
            y="independent"
        ),
        use_container_width=True,
    )

    # Monthly counts – all months in window
    st.markdown("**By month (all months in window)**")
    monthly = th_df.copy()
    monthly["year_month"] = (
        monthly["datetime_hour"].dt.to_period("M").dt.to_timestamp()
    )
    monthly_agg = monthly.groupby("year_month", as_index=False)[
        "price_eur_per_mwh"
    ].count()
    monthly_agg = monthly_agg.rename(columns={"price_eur_per_mwh": "hours"})
    monthly_chart = (
        alt.Chart(monthly_agg)
        .mark_bar()
        .encode(
            x=alt.X(
                "year_month:T",
                title="Month",
                axis=alt.Axis(format="%Y-%m", labelAngle=-45),
            ),
            y=alt.Y("hours:Q", title="Hours ≤ threshold"),
            tooltip=[
                alt.Tooltip("year_month:T", title="Month", format="%Y-%m"),
                alt.Tooltip("hours:Q", title="Hours"),
            ],
        )
        .properties(height=250)
    )
    st.altair_chart(monthly_chart, use_container_width=True)

    # Monthly counts – average calendar month (Jan–Dec) over years
    st.markdown("**By calendar month (average over years)**")
    monthly_by_year = th_df.copy()
    monthly_by_year["year"] = monthly_by_year["datetime_hour"].dt.year
    monthly_by_year["month"] = monthly_by_year["datetime_hour"].dt.month
    # hours per (year, month)
    ym_counts = (
        monthly_by_year.groupby(["year", "month"], as_index=False)["price_eur_per_mwh"]
        .count()
        .rename(columns={"price_eur_per_mwh": "hours"})
    )
    # average over years for each calendar month
    month_avg = (
        ym_counts.groupby("month", as_index=False)["hours"].mean().rename(
            columns={"hours": "hours_avg"}
        )
    )
    # Ensure all 12 calendar months are present, in order
    all_months = pd.DataFrame({"month": list(range(1, 13))})
    month_avg = all_months.merge(month_avg, on="month", how="left").fillna(0)
    month_avg["month_name"] = month_avg["month"].apply(
        lambda m: datetime(2000, m, 1).strftime("%b")
    )
    month_title = "Avg hours ≤ threshold per calendar month"
    month_chart = (
        alt.Chart(month_avg)
        .mark_bar()
        .encode(
            x=alt.X(
                "month_name:O",
                title="Calendar month",
                sort=[
                    "Jan",
                    "Feb",
                    "Mar",
                    "Apr",
                    "May",
                    "Jun",
                    "Jul",
                    "Aug",
                    "Sep",
                    "Oct",
                    "Nov",
                    "Dec",
                ],
            ),
            y=alt.Y("hours_avg:Q", title=month_title),
            tooltip=[
                alt.Tooltip("month_name:O", title="Month"),
                alt.Tooltip("hours_avg:Q", title="Avg hours", format=".1f"),
            ],
        )
        .properties(height=250)
    )
    st.altair_chart(month_chart, use_container_width=True)

    # Daily counts
    st.markdown("**By day**")
    daily = th_df.copy()
    daily["date"] = daily["datetime_hour"].dt.date
    daily_agg = daily.groupby("date", as_index=False)["price_eur_per_mwh"].count()
    daily_agg = daily_agg.rename(columns={"price_eur_per_mwh": "hours"})
    daily_chart = (
        alt.Chart(daily_agg)
        .mark_bar()
        .encode(
            x=alt.X(
                "date:T",
                title="Date",
                axis=alt.Axis(format="%Y-%m-%d", labelAngle=-45),
            ),
            y=alt.Y("hours:Q", title="Hours ≤ threshold"),
            tooltip=[
                alt.Tooltip("date:T", title="Date", format="%Y-%m-%d"),
                alt.Tooltip("hours:Q", title="Hours"),
            ],
        )
        .properties(height=250)
    )
    st.altair_chart(daily_chart, use_container_width=True)

    # Hour-of-day counts
    st.markdown("**By hour of day (0–23)**")
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

    y_title_h_hours = "Hours ≤ threshold"
    y_title_h_pct = "Hours ≤ threshold (% of hours in hour-of-day)"

    hourly_hours_chart = (
        alt.Chart(hourly)
        .mark_bar()
        .encode(
            x=alt.X("hour:O", title="Hour of day"),
            y=alt.Y("hours:Q", title=y_title_h_hours),
            tooltip=[
                alt.Tooltip("hour:O", title="Hour"),
                alt.Tooltip("hours:Q", title="Hours"),
                alt.Tooltip("percent:Q", title="Percent", format=".0f"),
            ],
        )
        .properties(height=250)
    )
    hourly_text = (
        alt.Chart(hourly)
        .mark_text(baseline="bottom", dy=-5)
        .encode(
            x="hour:O",
            y="hours:Q",
            text=alt.Text("hours:Q", format=".0f"),
        )
    )
    hourly_pct_chart = (
        alt.Chart(hourly)
        .mark_line(color="red")
        .encode(
            x=alt.X("hour:O", title="Hour of day"),
            y=alt.Y(
                "percent:Q",
                title=y_title_h_pct,
                axis=alt.Axis(titleColor="red", orient="right", format=".0f"),
            ),
        )
    )
    st.altair_chart(
        alt.layer(hourly_hours_chart, hourly_text, hourly_pct_chart).resolve_scale(
            y="independent"
        ),
        use_container_width=True,
    )


if __name__ == "__main__":
    main()


