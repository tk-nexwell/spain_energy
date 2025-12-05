"""
Shared session state management for data source and date range across all pages.
"""
from datetime import datetime
from typing import Optional

import pandas as pd
import streamlit as st

from data_loader import DataSource, get_data_source_date_range, get_default_date_range


def init_session_state():
    """Initialize session state variables if they don't exist."""
    if "data_source" not in st.session_state:
        st.session_state.data_source = "historical_prices"
    
    if "inflation_rate" not in st.session_state:
        st.session_state.inflation_rate = 0.0
    
    if "date_range_start" not in st.session_state:
        st.session_state.date_range_start = None
    
    if "date_range_end" not in st.session_state:
        st.session_state.date_range_end = None


def get_data_source_selector() -> DataSource:
    """
    Display data source selector in sidebar and return selected source.
    Updates session state.
    """
    init_session_state()
    
    # Standardized data source options in consistent order across all pages
    data_sources = {
        "OMIE DA SP (historical)": "omie_da",
        "ESIOS DA 600 (historical)": "historical_prices",
        "Aurora June 2025 (forecast)": "Aurora_Jun_2025",
        "Baringa Q2 2025 (forecast)": "Baringa_Q2_2025",
    }
    
    # Find current index based on session state
    current_source = st.session_state.data_source
    current_index = 0
    for i, (label, source) in enumerate(data_sources.items()):
        if source == current_source:
            current_index = i
            break
    
    selected_label = st.sidebar.radio(
        "Data Source",
        options=list(data_sources.keys()),
        index=current_index,
        key="data_source_selector",
    )
    
    selected_source: DataSource = data_sources[selected_label]
    
    # Update session state if changed
    if selected_source != st.session_state.data_source:
        st.session_state.data_source = selected_source
        # Reset date range when source changes
        st.session_state.date_range_start = None
        st.session_state.date_range_end = None
        # Also clear widget keys to force fresh defaults
        if "date_range_calendar" in st.session_state:
            del st.session_state.date_range_calendar
        if "date_range_slider" in st.session_state:
            del st.session_state.date_range_slider
    
    return selected_source


def get_inflation_input(source: DataSource) -> float:
    """
    Display inflation rate input for forecasts and return the rate.
    Updates session state.
    """
    init_session_state()
    
    if source in ("historical_prices", "omie_da"):
        return 0.0
    
    # Use session state if available, otherwise default to 2.0%
    default_rate = st.session_state.inflation_rate * 100.0 if st.session_state.inflation_rate > 0 else 2.0
    
    inflation_rate = st.sidebar.number_input(
        "Annual Inflation Rate (%)",
        min_value=0.0,
        max_value=10.0,
        value=default_rate,
        step=0.1,
        key="inflation_rate_input",
        help="Convert forecast prices from real to nominal terms. Set to 0% to keep real terms.",
    ) / 100.0  # Convert percentage to decimal
    
    st.session_state.inflation_rate = inflation_rate
    return inflation_rate


def get_date_range_selector(source: DataSource) -> tuple[datetime, datetime]:
    """
    Display date range selector with both calendar picker and slider.
    Both are synchronized - changing one updates the other.
    Updates session state and adapts to data source.
    
    Returns:
        Tuple of (start_datetime, end_datetime)
    """
    init_session_state()
    
    from datetime import time
    
    # Get date range for selected source
    min_dt, max_dt = get_data_source_date_range(source)
    min_date = min_dt.date()
    max_date = max_dt.date()
    
    # Get default date range based on source
    default_start_dt, default_end_dt = get_default_date_range(source, min_dt, max_dt)
    default_start_date = default_start_dt.date()
    default_end_date = default_end_dt.date()
    
    # Validate and use session state if available and valid for THIS source
    # Otherwise use defaults for the current source
    current_start = default_start_date
    current_end = default_end_date
    
    if (st.session_state.date_range_start is not None and 
        st.session_state.date_range_end is not None):
        try:
            # Validate session state dates are within range for THIS source
            session_start = st.session_state.date_range_start
            session_end = st.session_state.date_range_end
            
            # Check if dates are valid for the current source's date range
            if (isinstance(session_start, type(min_date)) and 
                isinstance(session_end, type(max_date)) and
                min_date <= session_start <= max_date and 
                min_date <= session_end <= max_date and
                session_start <= session_end):
                # Valid for current source, use them
                current_start = session_start
                current_end = session_end
            else:
                # Invalid for current source, reset to defaults
                st.session_state.date_range_start = None
                st.session_state.date_range_end = None
                # Clear widget keys to force fresh defaults
                if "date_range_calendar" in st.session_state:
                    del st.session_state.date_range_calendar
                if "date_range_slider" in st.session_state:
                    del st.session_state.date_range_slider
        except (AttributeError, TypeError, ValueError):
            # Invalid session state, reset and use defaults
            st.session_state.date_range_start = None
            st.session_state.date_range_end = None
            if "date_range_calendar" in st.session_state:
                del st.session_state.date_range_calendar
            if "date_range_slider" in st.session_state:
                del st.session_state.date_range_slider
    
    # Parse date range helper
    def parse_date_range(dr):
        if isinstance(dr, tuple) and len(dr) == 2:
            return dr[0], dr[1]
        elif isinstance(dr, tuple) and len(dr) == 1:
            return dr[0], dr[0]
        else:
            return dr, dr
    
    # Widget keys
    widget_calendar_key = "date_range_calendar"
    widget_slider_key = "date_range_slider"
    
    # Check widget keys against session state BEFORE creating widgets
    # If one widget's key differs from session state, it means it was changed
    # and we should sync the other widget's key before creating it
    widget_calendar_value = st.session_state.get(widget_calendar_key, None)
    widget_slider_value = st.session_state.get(widget_slider_key, None)
    
    # Determine which widget was changed by comparing widget keys to session state
    calendar_differs_from_state = False
    slider_differs_from_state = False
    
    if widget_calendar_value is not None:
        cal_widget_start, cal_widget_end = parse_date_range(widget_calendar_value)
        calendar_differs_from_state = (cal_widget_start != current_start or cal_widget_end != current_end)
    
    if widget_slider_value is not None:
        slider_widget_start, slider_widget_end = parse_date_range(widget_slider_value)
        slider_differs_from_state = (slider_widget_start != current_start or slider_widget_end != current_end)
    
    # If calendar differs from session state, sync slider key before creating slider
    if calendar_differs_from_state and widget_calendar_value is not None:
        cal_sync_start, cal_sync_end = parse_date_range(widget_calendar_value)
        # Update slider key to match calendar before creating slider
        st.session_state[widget_slider_key] = (cal_sync_start, cal_sync_end)
        # Also update session state to match
        st.session_state.date_range_start = cal_sync_start
        st.session_state.date_range_end = cal_sync_end
        current_start, current_end = cal_sync_start, cal_sync_end
    # If slider differs from session state, sync calendar key before creating calendar
    elif slider_differs_from_state and widget_slider_value is not None:
        slider_sync_start, slider_sync_end = parse_date_range(widget_slider_value)
        # Update calendar key to match slider before creating calendar
        st.session_state[widget_calendar_key] = (slider_sync_start, slider_sync_end)
        # Also update session state to match
        st.session_state.date_range_start = slider_sync_start
        st.session_state.date_range_end = slider_sync_end
        current_start, current_end = slider_sync_start, slider_sync_end
    
    # Calendar picker
    st.sidebar.markdown("**Date range (calendar)**")
    date_range_calendar = st.sidebar.date_input(
        "Select dates",
        value=(current_start, current_end),
        min_value=min_date,
        max_value=max_date,
        key=widget_calendar_key,
        help="Click to open calendar picker. Select start and end dates.",
        label_visibility="collapsed",
    )
    
    # Slider
    st.sidebar.markdown("**Date range (slider)**")
    date_range_slider = st.sidebar.slider(
        "Select date range",
        min_value=min_date,
        max_value=max_date,
        value=(current_start, current_end),
        format="YYYY-MM-DD",
        key=widget_slider_key,
        help="Drag to adjust date range quickly.",
        label_visibility="collapsed",
    )
    
    # Parse both widget values
    cal_start, cal_end = parse_date_range(date_range_calendar)
    slider_start, slider_end = parse_date_range(date_range_slider)
    
    # Determine which widget was changed by comparing to session state
    calendar_changed = (cal_start != current_start or cal_end != current_end)
    slider_changed = (slider_start != current_start or slider_end != current_end)
    
    # Use the widget that changed, or prefer calendar if both changed
    if calendar_changed:
        start_date, end_date = cal_start, cal_end
    elif slider_changed:
        start_date, end_date = slider_start, slider_end
    else:
        # Neither changed, use current session state
        start_date, end_date = current_start, current_end
    
    # Ensure we have valid dates
    if start_date is None:
        start_date = default_start_date
    if end_date is None:
        end_date = default_end_date
    
    # Update session state (source of truth)
    st.session_state.date_range_start = start_date
    st.session_state.date_range_end = end_date
    
    # Note: We cannot modify widget keys after widgets are created
    # The widgets will sync on the next rerun because they read from session state
    # via current_start/current_end which comes from date_range_start/date_range_end
    
    start_dt = datetime.combine(start_date, time.min)
    end_dt = datetime.combine(end_date, time.max)
    
    return start_dt, end_dt

