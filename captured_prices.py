from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
import sqlite3

# Import config to get INDICATORS
try:
    from config import INDICATORS
except ImportError:
    INDICATORS = {}


DATA_DIR = "data"
# Single unified DB for both prices and PV profiles
PRICES_DB = os.path.join(DATA_DIR, "data.db")
PV_DB = PRICES_DB


@dataclass
class MarketInfo:
    table: str
    datetime_col: str
    price_col: str
    label: str


def _connect(path: str) -> sqlite3.Connection:
    return sqlite3.connect(path)


def _get_table_name(indicator_id: int) -> str:
    """
    Return the SQLite table name for a given indicator in data.db.
    Matches the logic from db.py but for data.db.
    """
    if indicator_id == 600:
        return "historical_prices"
    return f"spot_prices_{indicator_id}"


def list_markets() -> Dict[str, MarketInfo]:
    """
    List available markets including both historical (spot_prices) and forecasts (aurora, baringa).
    
    Returns a dict mapping market IDs to MarketInfo objects.
    Markets are returned in standardized order:
    1) OMIE DA SP (historical)
    2) ESIOS DA 600 (historical)
    3) Aurora June 2025 (forecast)
    4) Baringa Q2 2025 (forecast)
    """
    markets: Dict[str, MarketInfo] = {}
    
    if not os.path.exists(PRICES_DB):
        return markets
    
    conn = _connect(PRICES_DB)
    cur = conn.cursor()
    
    # 1) Add OMIE DA prices first (standardized order)
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='historical_prices'")
    if cur.fetchone():
        cur.execute("PRAGMA table_info(historical_prices)")
        cols = [c[1] for c in cur.fetchall()]
        if "OMIE_SP_DA_prices" in cols:
            cur.execute("SELECT COUNT(*) FROM historical_prices WHERE OMIE_SP_DA_prices IS NOT NULL")
            if cur.fetchone()[0] > 0:
                markets["omie_da"] = MarketInfo(
                    table="historical_prices",
                    datetime_col="datetime",
                    price_col="OMIE_SP_DA_prices",
                    label="OMIE DA SP (historical)"
                )
    
    # Add historical spot prices
    if not INDICATORS:
        # Fallback: try to discover tables if INDICATORS is not available
        if not os.path.exists(PRICES_DB):
            return markets

        conn = _connect(PRICES_DB)
        cur = conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        tables = [r[0] for r in cur.fetchall()]

        for t in tables:
            cur.execute(f"PRAGMA table_info({t})")
            cols = cur.fetchall()
            if not cols:
                continue
            text_cols = [c[1] for c in cols if c[2].upper().startswith("TEXT")]
            real_cols = [
                c[1] for c in cols if c[2].upper() in ("REAL", "NUMERIC", "FLOAT", "DOUBLE")
            ]
            if text_cols and real_cols:
                dt_col = text_cols[0]
                price_col = real_cols[0]
                label = t.upper()
                markets[t] = MarketInfo(table=t, datetime_col=dt_col, price_col=price_col, label=label)

        conn.close()
        return markets
    
    # Use INDICATORS from config.py
    if not os.path.exists(PRICES_DB):
        return markets

    conn = _connect(PRICES_DB)
    cur = conn.cursor()
    
    for indicator_id, indicator_name in INDICATORS.items():
        table_name = _get_table_name(indicator_id)
        
        # Check if table exists
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
        if not cur.fetchone():
            continue
        
        # Get column info
        cur.execute(f"PRAGMA table_info({table_name})")
        cols = cur.fetchall()
        if not cols:
            continue
        
        text_cols = [c[1] for c in cols if c[2].upper().startswith("TEXT")]
        real_cols = [
            c[1] for c in cols if c[2].upper() in ("REAL", "NUMERIC", "FLOAT", "DOUBLE")
        ]
        all_col_names = [c[1] for c in cols]
        
        if text_cols and real_cols:
            dt_col = text_cols[0]
            
            # Determine the correct price column based on indicator ID
            # This matches the logic in pages/01_Real_Time_Prices.py load_data()
            if indicator_id == 600:
                # For indicator 600, use 'ESIOS_600_DA_prices' column
                if "ESIOS_600_DA_prices" in all_col_names:
                    price_col = "ESIOS_600_DA_prices"
                else:
                    # Fallback to first real column if day_ahead_prices doesn't exist
                    price_col = real_cols[0]
            else:
                # For other indicators (612, 613, 614), use column named after indicator ID
                indicator_col = str(indicator_id)
                if indicator_col in all_col_names:
                    price_col = indicator_col
                else:
                    # Fallback to first real column if indicator column doesn't exist
                    price_col = real_cols[0]
            
            # 2) Add ESIOS DA 600 with standardized label
            if indicator_id == 600:
                label = "ESIOS DA 600 (historical)"
            else:
                label = f"{indicator_id} – {indicator_name}"
            markets[str(indicator_id)] = MarketInfo(
                table=table_name, 
                datetime_col=dt_col, 
                price_col=price_col, 
                label=label
            )
    
    conn.close()
    
    # 3) Add forecast sources with standardized labels (after historical)
    if os.path.exists(PRICES_DB):
        conn = _connect(PRICES_DB)
        cur = conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='forecasts'")
        if cur.fetchone():
            for source_name, label in [
                ("Aurora_Jun_2025", "Aurora June 2025 (forecast)"),
                ("Baringa_Q2_2025", "Baringa Q2 2025 (forecast)")
            ]:
                cur.execute("SELECT COUNT(*) FROM forecasts WHERE source = ?", (source_name,))
                if cur.fetchone()[0] > 0:
                    markets[source_name] = MarketInfo(
                        table="forecasts",
                        datetime_col="datetime",
                        price_col="price_eur_per_mwh",
                        label=label
                    )
        conn.close()
    
    return markets


def list_pv_profiles() -> List[str]:
    """Return available PV profile column names from pv.db.pv_profiles."""
    if not os.path.exists(PV_DB):
        return []
    conn = _connect(PV_DB)
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(pv_profiles)")
    cols = [r[1] for r in cur.fetchall()]
    conn.close()
    # first three columns are month, day, hour
    return [c for c in cols if c not in ("month", "day", "hour")]


def load_price_series(
    market: str, 
    start_dt: pd.Timestamp | None = None, 
    end_dt: pd.Timestamp | None = None,
    inflation_rate: float = 0.0
) -> pd.DataFrame:
    """
    Load price series for a given market (historical or forecast).
    
    Args:
        market: Market identifier (e.g., "600" for historical, "Aurora_Jun_2025" or "Baringa_Q2_2025" for forecasts)
        start_dt: Optional start datetime filter
        end_dt: Optional end datetime filter
        inflation_rate: Annual inflation rate (0.0-1.0) for forecasts only
    
    Returns:
        DataFrame with price data
    """
    markets = list_markets()
    if market not in markets:
        raise ValueError(f"Unknown market '{market}'. Available: {list(markets.keys())}")
    info = markets[market]

    conn = _connect(PRICES_DB)
    
    # Handle forecasts vs historical differently
    if market in ["Aurora_Jun_2025", "Baringa_Q2_2025"]:
        # Forecast data
        df = pd.read_sql(
            "SELECT datetime, year, month, day, hour, minute, price_eur_per_mwh FROM forecasts WHERE source = ?",
            conn,
            params=(market,)
        )
    elif market == "omie_da":
        # OMIE DA historical data (Spain prices)
        df = pd.read_sql(
            "SELECT datetime, year, month, day, hour, minute, OMIE_SP_DA_prices as price_eur_per_mwh FROM historical_prices WHERE OMIE_SP_DA_prices IS NOT NULL",
            conn
        )
    else:
        # Historical data (ESIOS indicators)
        df = pd.read_sql(f"SELECT * FROM {info.table}", conn)
    
    conn.close()

    # Parse datetime and strip any timezone to avoid naive/aware comparison issues.
    # Use shared parse_timestamp utility function
    from utils import parse_timestamp
    
    dt_parsed = df["datetime"].apply(parse_timestamp)
    df["datetime"] = pd.to_datetime(dt_parsed, errors="coerce")
    df = df.dropna(subset=["datetime"]).copy()
    
    # Rename price column if needed (for historical data)
    if "price_eur_per_mwh" not in df.columns:
        df = df.rename(columns={info.price_col: "price_eur_per_mwh"})

    if start_dt is not None:
        df = df[df["datetime"] >= pd.to_datetime(start_dt)]
    if end_dt is not None:
        df = df[df["datetime"] <= pd.to_datetime(end_dt)]

    if df.empty:
        return df

    # Apply inflation to forecasts (need datetime_parsed for inflation calculation)
    if market in ["Aurora_Jun_2025", "Baringa_Q2_2025"] and inflation_rate > 0.0:
        from data_loader import apply_inflation_to_forecasts
        # Create datetime_parsed column for inflation calculation
        df["datetime_parsed"] = df["datetime"]
        df = apply_inflation_to_forecasts(df, inflation_rate)
        # Keep datetime_parsed for consistency with other code

    df["year"] = df["datetime"].dt.year
    df["month"] = df["datetime"].dt.month
    df["day"] = df["datetime"].dt.day
    df["hour"] = df["datetime"].dt.hour
    df["weekday"] = df["datetime"].dt.day_name()
    return df


def load_pv_profile(profile_col: str) -> pd.DataFrame:
    """Load a single PV profile as month, day, hour, pv_mwh."""
    if not os.path.exists(PV_DB):
        raise FileNotFoundError(PV_DB)
    conn = _connect(PV_DB)
    query = f"SELECT month, day, hour, {profile_col} AS pv_mwh FROM pv_profiles"
    df = pd.read_sql(query, conn)
    conn.close()
    df = df.dropna(subset=["pv_mwh"]).copy()
    df["month"] = df["month"].astype(int)
    df["day"] = df["day"].astype(int)
    df["hour"] = df["hour"].astype(int)
    return df


def join_price_with_pv(prices: pd.DataFrame, pv: pd.DataFrame) -> pd.DataFrame:
    """
    Join price time series with PV profile on (month, day, hour).
    
    Filters out rows where price_eur_per_mwh is NULL before joining,
    so that PV production is only included when there's a valid price.
    """
    if prices.empty or pv.empty:
        return prices.iloc[0:0].copy()

    # Filter out NULL prices before joining
    # This ensures PV production is only included when there's a valid price
    prices = prices[prices["price_eur_per_mwh"].notna()].copy()
    
    if prices.empty:
        return prices

    pv_keys = pv[["month", "day"]].drop_duplicates()
    has_feb29 = ((pv_keys["month"] == 2) & (pv_keys["day"] == 29)).any()
    if not has_feb29:
        prices = prices[~((prices["month"] == 2) & (prices["day"] == 29))].copy()

    merged = prices.merge(
        pv[["month", "day", "hour", "pv_mwh"]],
        on=["month", "day", "hour"],
        how="inner",
    )
    if merged.empty:
        return merged

    merged["pv_weighted_price_component"] = merged["price_eur_per_mwh"] * merged["pv_mwh"]
    return merged


def compute_captured_price_aggregations(
    df: pd.DataFrame, group_cols: List[str]
) -> pd.DataFrame:
    """
    Compute PV-weighted captured price over arbitrary groupings.

    captured_price = sum(price * pv_mwh) / sum(pv_mwh)
    
    Returns a DataFrame with group_cols and captured_price column.
    """
    if df.empty:
        return pd.DataFrame(columns=group_cols + ["captured_price"])

    # Compute sum of (price * pv) and sum of pv for each group
    grouped = df.groupby(group_cols, as_index=False).agg(
        sum_price_pv=("pv_weighted_price_component", "sum"),
        sum_pv=("pv_mwh", "sum"),
    )
    
    # Keep only groups with positive PV output and compute captured price
    grouped = grouped[grouped["sum_pv"] > 0].copy()
    grouped["captured_price"] = grouped["sum_price_pv"] / grouped["sum_pv"]
    
    return grouped[group_cols + ["captured_price"]]


def compute_typical_day_profiles() -> pd.DataFrame:
    """
    Compute average PV output by hour across the synthetic year for each profile.
    Returns a long DataFrame with columns: hour, profile, pv_mwh.
    """
    profiles = list_pv_profiles()
    if not profiles:
        return pd.DataFrame(columns=["hour", "profile", "pv_mwh"])

    conn = _connect(PV_DB)
    df = pd.read_sql("SELECT * FROM pv_profiles", conn)
    conn.close()

    long_rows = []
    for p in profiles:
        if p not in df.columns:
            continue
        tmp = (
            df[["hour", p]].dropna().groupby("hour", as_index=False)[p].mean()
        )  # average over all days
        tmp["profile"] = p
        tmp = tmp.rename(columns={p: "pv_mwh"})
        long_rows.append(tmp)

    if not long_rows:
        return pd.DataFrame(columns=["hour", "profile", "pv_mwh"])

    out = pd.concat(long_rows, ignore_index=True)
    out["hour"] = out["hour"].astype(int)
    return out


