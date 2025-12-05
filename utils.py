"""
Shared utility functions for the spain_energy project.
"""
from datetime import datetime
from typing import Optional


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

