"""
Shared utility functions for the spain_energy project.
"""
from datetime import datetime
from typing import Optional

import pandas as pd


def parse_timestamp(ts: str) -> Optional[datetime]:
    """
    Parse timestamp strings from the DB into naive datetimes
    without shifting the wall-clock time.
    
    Handles both 'Z' (UTC) and '+01:00' (timezone offset) formats.
    
    Examples:
    - '2015-01-01T01:00:00+01:00' -> datetime(2015, 1, 1, 1, 0, 0)
    - '2015-01-01T00:00:00Z' -> datetime(2015, 1, 1, 0, 0, 0)
    - '2015-01-01T00:00:00.000+01:00' -> datetime(2015, 1, 1, 0, 0, 0)
    
    Args:
        ts: Timestamp string from database
        
    Returns:
        Naive datetime object, or None if parsing fails
    """
    s = str(ts)
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except (ValueError, AttributeError):
        return None
    # Drop tzinfo so we can safely compare with naive datetimes from Streamlit.
    return dt.replace(tzinfo=None)


def format_datetime_for_csv(df: pd.DataFrame, datetime_col: str = "datetime") -> pd.DataFrame:
    """
    Format datetime column for CSV export to ensure consistent format.
    
    Ensures datetime column is exported as 'YYYY-MM-DD HH:MM:SS' format
    instead of ISO format with 'T'.
    
    Args:
        df: DataFrame to format
        datetime_col: Name of the datetime column to format
        
    Returns:
        DataFrame with datetime column formatted as string
    """
    df = df.copy()
    if datetime_col in df.columns:
        # If datetime column exists, ensure it's a string in the correct format
        if pd.api.types.is_datetime64_any_dtype(df[datetime_col]):
            # Convert datetime to string format
            df[datetime_col] = df[datetime_col].dt.strftime('%Y-%m-%d %H:%M:%S')
        else:
            # Already a string, but ensure format is correct
            # Convert any ISO format (with T) to space format
            def format_datetime_str(ts_str):
                if pd.isna(ts_str):
                    return ''
                ts_str = str(ts_str)
                # If already in correct format (has space, no T), return as-is
                if ' ' in ts_str and 'T' not in ts_str:
                    return ts_str
                # Replace T with space for ISO format
                if 'T' in ts_str:
                    ts_str = ts_str.replace('T', ' ')
                # Remove timezone and microseconds if present
                ts_str = ts_str.replace('Z', '').split('+')[0].split('.')[0].strip()
                return ts_str
            
            df[datetime_col] = df[datetime_col].apply(format_datetime_str)
    return df

