"""
Centralized chart configuration for consistent formatting across all pages.
"""
from typing import Literal, Optional

import altair as alt
import pandas as pd

# Chart order constants
CHART_ORDER = [
    "yearly",
    "year_month",  # All months in period
    "calendar_month",  # Jan-Dec average
    "daily",  # Daily average line
    "day_of_week",
    "hour_of_day",
]

# Standardized chart titles across all pages
CHART_TITLES = {
    "yearly": "By Year",
    "year_month": "By Month (all months in window)",
    "calendar_month": "By Calendar-Month (average over years)",
    "daily": "By Day",
    "day_of_week": "By Day-of-Week",
    "hour_of_day": "By Hour of Day (0–23)",
}

# PV Captured Prices titles (same as standard)
PV_CAPTURED_TITLES = CHART_TITLES.copy()

# PPA Effective Price titles (same as standard)
PPA_EFFECTIVE_TITLES = CHART_TITLES.copy()

# PV Captured Factor titles (same as standard)
PV_CAPTURED_FACTOR_TITLES = CHART_TITLES.copy()

# Common formatting
MONTH_ORDER = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
DAY_ORDER = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

# Brand color: RGB(67, 150, 68) = #439644
BRAND_COLOR = "#439644"


def get_chart_title(chart_type: str, page_type: Literal["prices", "pv_captured", "ppa_effective", "pv_captured_factor"] = "prices") -> str:
    """Get standardized chart title based on chart type and page type."""
    if page_type == "pv_captured":
        return PV_CAPTURED_TITLES.get(chart_type, CHART_TITLES.get(chart_type, ""))
    elif page_type == "ppa_effective":
        return PPA_EFFECTIVE_TITLES.get(chart_type, CHART_TITLES.get(chart_type, ""))
    elif page_type == "pv_captured_factor":
        return PV_CAPTURED_FACTOR_TITLES.get(chart_type, CHART_TITLES.get(chart_type, ""))
    else:
        return CHART_TITLES.get(chart_type, "")


def create_yearly_chart(
    df: pd.DataFrame,
    value_col: str = "price_eur_per_mwh",
    value_title: str = "Average price (€/MWh)",
    show_labels: bool = True,
) -> alt.Chart:
    """Create standardized yearly average bar chart."""
    base = (
        alt.Chart(df)
        .mark_bar(color=BRAND_COLOR)
        .encode(
            x=alt.X("year:O", title="Year"),
            y=alt.Y(f"{value_col}:Q", title=value_title if value_title else "€/MWh"),
            tooltip=[
                alt.Tooltip("year:O", title="Year"),
                alt.Tooltip(f"{value_col}:Q", title="Average", format=".2f"),
            ],
        )
        .properties(height=250)
    )
    
    if show_labels:
        labels = (
            alt.Chart(df)
            .mark_text(baseline="bottom", dy=-5)
            .encode(
                x="year:O",
                y=f"{value_col}:Q",
                text=alt.Text(f"{value_col}:Q", format=".1f"),
            )
        )
        return base + labels
    return base


def create_year_month_chart(
    df: pd.DataFrame,
    value_col: str = "price_eur_per_mwh",
    value_title: str = "Average price (€/MWh)",
    show_labels: bool = False,
) -> alt.Chart:
    """Create standardized year-month bar chart."""
    chart = (
        alt.Chart(df)
        .mark_bar(color=BRAND_COLOR)
        .encode(
            x=alt.X(
                "year_month:T",
                title="Month",
                axis=alt.Axis(format="%b-%y", labelAngle=-45),
            ),
            y=alt.Y(f"{value_col}:Q", title=value_title if value_title else "€/MWh"),
            tooltip=[
                alt.Tooltip("year_month:T", title="Month", format="%Y-%m"),
                alt.Tooltip(f"{value_col}:Q", title="Average", format=".2f"),
            ],
        )
        .properties(height=250)
    )
    return chart


def create_calendar_month_chart(
    df: pd.DataFrame,
    value_col: str = "price_eur_per_mwh",
    value_title: str = "Average price (€/MWh)",
    month_col: str = "month_name",
    show_labels: bool = True,
) -> alt.Chart:
    """Create standardized calendar month bar chart (Jan-Dec)."""
    base = (
        alt.Chart(df)
        .mark_bar(color=BRAND_COLOR)
        .encode(
            x=alt.X(
                f"{month_col}:O",
                title="Calendar month",
                sort=MONTH_ORDER,
            ),
            y=alt.Y(f"{value_col}:Q", title=value_title if value_title else "€/MWh"),
            tooltip=[
                alt.Tooltip(f"{month_col}:O", title="Month"),
                alt.Tooltip(f"{value_col}:Q", title="Average", format=".2f"),
            ],
        )
        .properties(height=250)
    )
    
    if show_labels:
        labels = (
            alt.Chart(df)
            .mark_text(baseline="bottom", dy=-5)
            .encode(
                x=alt.X(
                    f"{month_col}:O",
                    sort=MONTH_ORDER
                ),
                y=f"{value_col}:Q",
                text=alt.Text(f"{value_col}:Q", format=".1f"),
            )
        )
        return base + labels
    return base


def create_daily_chart(
    df: pd.DataFrame,
    value_col: str = "price_eur_per_mwh",
    value_title: str = "Average price (€/MWh)",
    date_col: str = "date",
) -> alt.Chart:
    """Create standardized daily average line chart."""
    chart = (
        alt.Chart(df)
        .mark_line(color=BRAND_COLOR)
        .encode(
            x=alt.X(
                f"{date_col}:T",
                title="Date",
                axis=alt.Axis(format="%b-%y", labelAngle=-45),
            ),
            y=alt.Y(f"{value_col}:Q", title=value_title if value_title else "€/MWh"),
            tooltip=[
                alt.Tooltip(f"{date_col}:T", title="Date", format="%Y-%m-%d"),
                alt.Tooltip(f"{value_col}:Q", title="Average", format=".2f"),
            ],
        )
        .properties(height=250)
    )
    return chart


def create_day_of_week_chart(
    df: pd.DataFrame,
    value_col: str = "price_eur_per_mwh",
    value_title: str = "Average price (€/MWh)",
    weekday_col: str = "weekday",
    show_labels: bool = True,
) -> alt.Chart:
    """Create standardized day-of-week bar chart (Mon-Sun)."""
    base = (
        alt.Chart(df)
        .mark_bar(color=BRAND_COLOR)
        .encode(
            x=alt.X(
                f"{weekday_col}:O",
                title="Day of week",
                sort=DAY_ORDER,
            ),
            y=alt.Y(f"{value_col}:Q", title=value_title if value_title else "€/MWh"),
            tooltip=[
                alt.Tooltip(f"{weekday_col}:O", title="Day"),
                alt.Tooltip(f"{value_col}:Q", title="Average", format=".2f"),
            ],
        )
        .properties(height=250)
    )
    
    if show_labels:
        labels = (
            alt.Chart(df)
            .mark_text(baseline="bottom", dy=-5)
            .encode(
                x=alt.X(
                    f"{weekday_col}:O",
                    sort=DAY_ORDER
                ),
                y=f"{value_col}:Q",
                text=alt.Text(f"{value_col}:Q", format=".1f"),
            )
        )
        return base + labels
    return base


def create_hour_of_day_chart(
    df: pd.DataFrame,
    value_col: str = "price_eur_per_mwh",
    value_title: str = "Average price (€/MWh)",
    hour_col: str = "hour",
    show_labels: bool = True,
) -> alt.Chart:
    """Create standardized hour-of-day bar chart (0-23)."""
    base = (
        alt.Chart(df)
        .mark_bar(color=BRAND_COLOR)
        .encode(
            x=alt.X(f"{hour_col}:O", title="Hour of day"),
            y=alt.Y(f"{value_col}:Q", title=value_title if value_title else "€/MWh"),
            tooltip=[
                alt.Tooltip(f"{hour_col}:O", title="Hour"),
                alt.Tooltip(f"{value_col}:Q", title="Average", format=".2f"),
            ],
        )
        .properties(height=250)
    )
    
    if show_labels:
        labels = (
            alt.Chart(df)
            .mark_text(baseline="bottom", dy=-5)
            .encode(
                x=f"{hour_col}:O",
                y=f"{value_col}:Q",
                text=alt.Text(f"{value_col}:Q", format=".1f"),
            )
        )
        return base + labels
    return base


def ensure_all_months(df: pd.DataFrame, month_col: str = "month") -> pd.DataFrame:
    """Ensure all 12 calendar months are present in dataframe."""
    all_months = pd.DataFrame({month_col: list(range(1, 13))})
    df = all_months.merge(df, on=month_col, how="left")
    return df.fillna(0.0)


def ensure_all_days(df: pd.DataFrame, weekday_col: str = "weekday", weekday_order_col: str = "weekday_order") -> pd.DataFrame:
    """Ensure all 7 weekdays are present in dataframe (Mon-Sun)."""
    all_days = pd.DataFrame({
        weekday_col: DAY_ORDER,
        weekday_order_col: list(range(7))
    })
    df = all_days.merge(df, on=[weekday_col, weekday_order_col], how="left")
    df = df.fillna(0.0)
    df = df.sort_values(weekday_order_col)
    return df


def ensure_all_hours(df: pd.DataFrame, hour_col: str = "hour") -> pd.DataFrame:
    """Ensure all 24 hours (0-23) are present in dataframe."""
    all_hours = pd.DataFrame({hour_col: list(range(24))})
    df = all_hours.merge(df, on=hour_col, how="left")
    return df.fillna(0.0)


def create_multi_series_bar_chart(
    df: pd.DataFrame,
    x_col: str,
    value_col: str,
    series_col: str,
    x_title: str,
    y_title: str,
    show_labels: bool = False,
    x_sort: Optional[list] = None,
) -> alt.Chart:
    """
    Create a multi-series bar chart with all series on the same chart.
    Series visibility is controlled by filtering the dataframe before calling this function.
    
    Args:
        df: DataFrame with melted data (x_col, value_col, series_col)
        x_col: Column name for x-axis
        value_col: Column name for y-axis values
        series_col: Column name for series grouping
        x_title: X-axis title
        y_title: Y-axis title
        show_labels: Whether to show data labels on bars
        x_sort: Optional list to sort x-axis categories
    
    Returns:
        Altair chart with multi-series bars (grouped bars)
    """
    base = (
        alt.Chart(df)
        .mark_bar()
        .encode(
            x=alt.X(
                f"{x_col}:O",
                title=x_title,
                sort=x_sort if x_sort else None,
            ),
            y=alt.Y(f"{value_col}:Q", title=y_title),
            color=alt.Color(
                f"{series_col}:N",
                scale=alt.Scale(
                    domain=["Charge Price", "Discharge Price", "Spread"],
                    range=[BRAND_COLOR, "#E74C3C", "#3498DB"]
                ),
                legend=alt.Legend(title="Metric"),
            ),
            tooltip=[
                alt.Tooltip(f"{x_col}:O", title=x_title),
                alt.Tooltip(f"{series_col}:N", title="Metric"),
                alt.Tooltip(f"{value_col}:Q", title="Price", format=".2f"),
            ],
        )
        .properties(height=250)
    )
    
    if show_labels:
        labels = (
            alt.Chart(df)
            .mark_text(baseline="bottom", dy=-5)
            .encode(
                x=alt.X(
                    f"{x_col}:O",
                    sort=x_sort if x_sort else None,
                ),
                y=f"{value_col}:Q",
                text=alt.Text(f"{value_col}:Q", format=".1f"),
            )
        )
        return base + labels
    return base


def create_multi_series_line_chart(
    df: pd.DataFrame,
    x_col: str,
    value_col: str,
    series_col: str,
    x_title: str,
    y_title: str,
    show_points: bool = False,
    x_format: Optional[str] = None,
) -> alt.Chart:
    """
    Create a multi-series line chart with toggleable series visibility.
    
    Args:
        df: DataFrame with melted data (x_col, value_col, series_col)
        x_col: Column name for x-axis
        value_col: Column name for y-axis values
        series_col: Column name for series grouping
        x_title: X-axis title
        y_title: Y-axis title
        show_points: Whether to show point markers
        x_format: Optional date format string for x-axis
    
    Returns:
        Altair chart with multi-series lines
    """
    # Determine x-axis type based on format
    is_temporal = x_format is not None
    x_type = "T" if is_temporal else "O"
    
    chart = (
        alt.Chart(df)
        .mark_line(point=show_points)
        .encode(
            x=alt.X(
                f"{x_col}:{x_type}",
                title=x_title,
                axis=alt.Axis(format=x_format, labelAngle=-45) if x_format else alt.Axis(),
            ),
            y=alt.Y(f"{value_col}:Q", title=y_title),
            color=alt.Color(
                f"{series_col}:N",
                scale=alt.Scale(
                    domain=["Charge Price", "Discharge Price", "Spread"],
                    range=[BRAND_COLOR, "#E74C3C", "#3498DB"]
                ),
                legend=alt.Legend(title="Metric"),
            ),
            tooltip=[
                alt.Tooltip(f"{x_col}:{x_type}", title=x_title, format=x_format if x_format else None),
                alt.Tooltip(f"{series_col}:N", title="Metric"),
                alt.Tooltip(f"{value_col}:Q", title="Price", format=".2f"),
            ],
        )
        .properties(height=250)
    )
    return chart

