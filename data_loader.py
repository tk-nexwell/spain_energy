"""
Unified data loading module for both historical and forecast data.
"""
import sqlite3
from datetime import datetime
from typing import Literal, Optional

import pandas as pd

from db import DB_PATH
from utils import parse_timestamp

DataSource = Literal["historical_prices", "omie_da", "Aurora_Jun_2025", "Baringa_Q2_2025"]


def apply_inflation_to_forecasts(df: pd.DataFrame, inflation_rate: float, base_date: Optional[pd.Timestamp] = None) -> pd.DataFrame:
    """
    Apply inflation adjustment to forecast prices to convert from real to nominal terms.
    
    Args:
        df: DataFrame with forecast data (must have datetime_parsed column)
        inflation_rate: Annual inflation rate as a decimal (e.g., 0.02 for 2%)
        base_date: Base date for inflation calculation (defaults to today)
    
    Returns:
        DataFrame with price_eur_per_mwh adjusted for inflation
    """
    if df.empty or inflation_rate == 0.0:
        return df
    
    df = df.copy()
    
    if base_date is None:
        base_date = pd.Timestamp.now()
    
    # Calculate years from base date for each row
    df["years_from_base"] = (df["datetime_parsed"] - base_date).dt.total_seconds() / (365.25 * 24 * 3600)
    
    # Apply inflation: nominal_price = real_price * (1 + rate)^years
    df["price_eur_per_mwh"] = df["price_eur_per_mwh"] * ((1 + inflation_rate) ** df["years_from_base"])
    
    # Drop the helper column
    df = df.drop(columns=["years_from_base"])
    
    return df


def load_price_data(
    source: DataSource,
    start_dt: Optional[pd.Timestamp] = None,
    end_dt: Optional[pd.Timestamp] = None,
    inflation_rate: float = 0.0,
) -> pd.DataFrame:
    """
    Load price data from either historical (historical_prices) or forecast (aurora/baringa) sources.
    
    Args:
        source: Data source - "historical_prices" for historical, "Aurora_Jun_2025" or "Baringa_Q2_2025" for forecasts
        start_dt: Optional start datetime filter
        end_dt: Optional end datetime filter
    
    Returns:
        DataFrame with columns: datetime, datetime_parsed, year, month, day, hour, minute, price_eur_per_mwh
    """
    conn = sqlite3.connect(DB_PATH)
    
    try:
        if source == "historical_prices":
            # Load historical ESIOS 600 prices
            df = pd.read_sql(
                "SELECT datetime, year, month, day, hour, minute, ESIOS_600_DA_prices as price_eur_per_mwh "
                "FROM historical_prices WHERE ESIOS_600_DA_prices IS NOT NULL ORDER BY datetime",
                conn
            )
        elif source == "omie_da":
            # Load historical OMIE day-ahead prices (Spain prices)
            df = pd.read_sql(
                "SELECT datetime, year, month, day, hour, minute, OMIE_SP_DA_prices as price_eur_per_mwh "
                "FROM historical_prices WHERE OMIE_SP_DA_prices IS NOT NULL ORDER BY datetime",
                conn
            )
        else:
            # Load forecast data (aurora or baringa)
            df = pd.read_sql(
                "SELECT datetime, year, month, day, hour, minute, price_eur_per_mwh, source "
                "FROM forecasts WHERE source = ? ORDER BY datetime",
                conn,
                params=(source,)
            )
    except Exception:
        return pd.DataFrame()
    finally:
        conn.close()
    
    if df.empty:
        return df
    
    # Parse datetime using shared utility
    parsed = df["datetime"].apply(parse_timestamp)
    df = df[parsed.notna()].copy()
    df["datetime_parsed"] = parsed[parsed.notna()]
    
    # Apply date filters if provided
    if start_dt is not None:
        df = df[df["datetime_parsed"] >= pd.to_datetime(start_dt)]
    if end_dt is not None:
        df = df[df["datetime_parsed"] <= pd.to_datetime(end_dt)]
    
    # Apply inflation to forecasts (not historical data)
    if source not in ("historical_prices", "omie_da") and inflation_rate > 0.0:
        df = apply_inflation_to_forecasts(df, inflation_rate)
    
    return df


def get_data_source_date_range(source: DataSource) -> tuple[pd.Timestamp, pd.Timestamp]:
    """
    Get the min and max datetime available for a given data source.
    
    Returns:
        Tuple of (min_datetime, max_datetime)
    """
    conn = sqlite3.connect(DB_PATH)
    
    try:
        if source == "historical_prices":
            query = "SELECT MIN(datetime) as min_dt, MAX(datetime) as max_dt FROM historical_prices WHERE ESIOS_600_DA_prices IS NOT NULL"
            df = pd.read_sql(query, conn)
        elif source == "omie_da":
            query = "SELECT MIN(datetime) as min_dt, MAX(datetime) as max_dt FROM historical_prices WHERE OMIE_SP_DA_prices IS NOT NULL"
            df = pd.read_sql(query, conn)
        else:
            query = "SELECT MIN(datetime) as min_dt, MAX(datetime) as max_dt FROM forecasts WHERE source = ?"
            params = (source,)
            df = pd.read_sql(query, conn, params=params)
        
        if df.empty or df.iloc[0]["min_dt"] is None:
            return pd.Timestamp("2018-01-01"), pd.Timestamp.now()
        
        min_str = str(df.iloc[0]["min_dt"])
        max_str = str(df.iloc[0]["max_dt"])
        
        min_dt = parse_timestamp(min_str)
        max_dt = parse_timestamp(max_str)
        
        if min_dt is None or max_dt is None:
            return pd.Timestamp("2018-01-01"), pd.Timestamp.now()
        
        return pd.Timestamp(min_dt), pd.Timestamp(max_dt)
    except Exception:
        return pd.Timestamp("2018-01-01"), pd.Timestamp.now()
    finally:
        conn.close()


def get_default_date_range(source: DataSource, min_dt: pd.Timestamp, max_dt: pd.Timestamp) -> tuple[pd.Timestamp, pd.Timestamp]:
    """
    Get the default date range for a data source.
    
    For historical (spot_prices): Year-to-date (YTD) from Jan 1 of current year
    For forecasts: Next 10 years from today
    
    Returns:
        Tuple of (start_datetime, end_datetime)
    """
    from datetime import datetime, timedelta
    
    if source in ("historical_prices", "omie_da"):
        # Historical: YTD default
        current_year = datetime.now().year
        ytd_start = datetime(current_year, 1, 1)
        start_dt = max(pd.Timestamp(ytd_start), min_dt)
        end_dt = max_dt
    else:
        # Forecasts: Next 10 years from today
        today = pd.Timestamp.now().normalize()
        start_dt = max(today, min_dt)
        end_dt = min(today + pd.Timedelta(days=3650), max_dt)  # 10 years
    
    return start_dt, end_dt

