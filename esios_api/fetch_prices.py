import pandas as pd
from .client import get_indicator_data
from .config import PENINSULA_GEO_NAME

def fetch_day_ahead_prices(indicator_id: int, start: str, end: str) -> pd.DataFrame:
    data = get_indicator_data(indicator_id, start, end)
    values = data["indicator"]["values"]
    df = pd.DataFrame(values)

    # Filter for mainland Spain
    df = df[df["geo_name"] == PENINSULA_GEO_NAME].copy()

    # Parse datetime and clean up
    df["datetime"] = pd.to_datetime(df["datetime"])
    df = df[["datetime", "value"]].rename(columns={"value": "price_eur_per_mwh"})
    df.sort_values("datetime", inplace=True)

    return df
