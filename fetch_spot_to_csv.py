import argparse
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

from config import EsiosConfig, ESIOS_API_TOKEN, INDICATORS
from esios_client import get_indicator_data
from db import init_db, insert_prices, get_latest_datetime


def transform_indicator_values(values: list[dict]) -> pd.DataFrame:
    """Normalize raw ESIOS indicator values into our standard DataFrame shape."""
    df = pd.DataFrame(values)

    # Filter for mainland Spain and clean up.
    # Depending on the ESIOS response, we may get either `geo_name` or just `geo_id`.
    if "geo_name" in df.columns:
        df = df[df["geo_name"].isin(["España", "Península"])].copy()
    elif "geo_id" in df.columns:
        # In the geos list, geo_id 3 corresponds to "España" (mainland Spain).
        df = df[df["geo_id"] == 3].copy()

    # Choose a datetime column and preserve the original timestamp string.
    if "datetime" in df.columns:
        ts_col = "datetime"
    elif "datetime_utc" in df.columns:
        ts_col = "datetime_utc"
    else:
        raise SystemExit("No datetime field found in ESIOS response.")

    # Keep the raw timestamp exactly as returned by the API so there is no shift.
    df["timestamp"] = df[ts_col].astype(str)

    # Parse per-row using Python datetime, without changing the label or forcing UTC.
    def _parse_ts(s: str):
        s = str(s)
        # Normalise trailing 'Z' to '+00:00' so datetime.fromisoformat can handle it.
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        try:
            return datetime.fromisoformat(s)
        except ValueError:
            return None

    parsed = df["timestamp"].apply(_parse_ts)
    mask = parsed.notna()
    df = df[mask].copy()
    parsed = parsed[mask]

    # Add nicer time breakdown for visualization / querying, based on the parsed datetimes.
    df["year"] = parsed.apply(lambda d: d.year)
    df["month"] = parsed.apply(lambda d: d.month)
    df["day"] = parsed.apply(lambda d: d.day)
    df["hour"] = parsed.apply(lambda d: d.hour)
    df["minute"] = parsed.apply(lambda d: d.minute)

    # Keep only what's useful
    if "value" not in df.columns:
        raise SystemExit("No 'value' field found in ESIOS response.")

    df = df[
        ["timestamp", "year", "month", "day", "hour", "minute", "value"]
    ].copy()
    
    # Standardize datetime format to "YYYY-MM-DD HH:MM:SS"
    def standardize_datetime(ts_str):
        """Convert datetime string to standardized format."""
        ts = str(ts_str)
        # Handle ISO format with T and Z
        if "T" in ts:
            ts = ts.replace("T", " ")
        if ts.endswith("Z"):
            ts = ts[:-1]
        if "+" in ts:
            ts = ts.split("+")[0]
        if "-" in ts and len(ts.split("-")) > 3:
            # Remove timezone offset
            parts = ts.split("-")
            if len(parts) > 3:
                ts = "-".join(parts[:3]) + " " + parts[-1].split(" ")[-1] if " " in parts[-1] else ts
        
        # Parse and reformat
        try:
            formats = [
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%dT%H:%M:%SZ",
                "%Y-%m-%d %H:%M:%S.%f",
                "%Y-%m-%dT%H:%M:%S.%fZ",
            ]
            for fmt in formats:
                try:
                    dt = datetime.strptime(ts, fmt)
                    return dt.strftime("%Y-%m-%d %H:%M:%S")
                except ValueError:
                    continue
            # Fallback to fromisoformat
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            if dt.tzinfo:
                dt = dt.replace(tzinfo=None)
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            return ts  # Return original if parsing fails
    
    df["datetime"] = df["timestamp"].apply(standardize_datetime)
    df = df.rename(columns={"value": "price_eur_per_mwh"})
    df = df[["datetime", "year", "month", "day", "hour", "minute", "price_eur_per_mwh"]]
    df.sort_values("datetime", inplace=True)
    return df


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Fetch ESIOS spot prices (indicator 600) and save to CSV.\n"
            "Uses ESIOS' native resolution: hourly historically, 15-minute "
            "from the SDAC go-live onwards."
        )
    )
    parser.add_argument(
        "--start",
        type=str,
        help=(
            "Start date (YYYY-MM-DD). "
            "If omitted, we continue from the last timestamp in the database."
        ),
    )
    parser.add_argument(
        "--end",
        type=str,
        help="End date (YYYY-MM-DD). Defaults to now (UTC).",
    )
    parser.add_argument(
        "--out",
        type=str,
        default="esios_spot.csv",
        help="Output CSV path (default: esios_spot.csv).",
    )
    parser.add_argument(
        "--indicator",
        type=int,
        choices=list(INDICATORS.keys()),
        default=EsiosConfig().spot_indicator_id,
        help=(
            "ESIOS indicator ID to fetch "
            f"(default: {EsiosConfig().spot_indicator_id}). "
            "Supported options: "
            + ", ".join(f"{k} ({v})" for k, v in INDICATORS.items())
        ),
    )
    return parser.parse_args()


def main() -> None:
    if not ESIOS_API_TOKEN:
        raise SystemExit(
            "ESIOS_API_TOKEN is not set. Create a .env with ESIOS_API_TOKEN=... "
            "or export it in your environment."
        )

    args = parse_args()

    indicator_id = args.indicator

    # Ensure database and table exist before we try to read from them
    init_db(indicator_id)
    now_utc = datetime.now(timezone.utc)

    if args.start:
        start_date = datetime.fromisoformat(args.start).replace(tzinfo=timezone.utc)
    else:
        # Incremental mode: continue from the last timestamp we have in the DB.
        last_str = get_latest_datetime(indicator_id)
        if last_str:
            last_dt = datetime.fromisoformat(last_str.replace("Z", "+00:00"))
            # Move a bit past the last stored point so we don't refetch it.
            start_date = last_dt + timedelta(minutes=15)
        else:
            # No data yet: default to Jan 1 of current year.
            start_date = datetime(now_utc.year, 1, 1, tzinfo=timezone.utc)

    if args.end:
        end_date = datetime.fromisoformat(args.end).replace(tzinfo=timezone.utc)
    else:
        end_date = now_utc

    start_iso = start_date.isoformat().replace("+00:00", "Z")
    end_iso = end_date.isoformat().replace("+00:00", "Z")

    print(f"Fetching indicator {indicator_id} from {start_iso} to {end_iso}...")

    # Do NOT set time_trunc here: we want native resolution from ESIOS.
    # - Before the SDAC 15-minute go-live: 1 price per hour
    # - After the go-live: 4 prices per hour (15-minute)
    data = get_indicator_data(indicator_id, start=start_iso, end=end_iso)

    df = transform_indicator_values(data["indicator"]["values"])

    out_path = Path(args.out)
    df.to_csv(out_path, index=False)

    # Also store into SQLite for easier querying
    init_db(indicator_id)
    insert_prices(df, indicator_id)

    print(
        f"Saved {len(df)} rows to {out_path} "
        f"and into data/data.db table for indicator {indicator_id}."
    )


if __name__ == "__main__":
    main()


