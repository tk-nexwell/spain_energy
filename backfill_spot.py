import argparse
import time
from datetime import datetime, timedelta, timezone

from requests.exceptions import HTTPError

from config import EsiosConfig, ESIOS_API_TOKEN, INDICATORS
from db import init_db, insert_prices
from esios_client import get_indicator_data
from fetch_spot_to_csv import transform_indicator_values


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Backfill ESIOS spot prices (indicator 600) into the local database "
            "in small chunks, to avoid overwhelming the API."
        )
    )
    parser.add_argument(
        "--start",
        type=str,
        required=True,
        help="Start date (YYYY-MM-DD), e.g. 2018-01-01",
    )
    parser.add_argument(
        "--end",
        type=str,
        required=True,
        help="End date (YYYY-MM-DD), e.g. 2025-12-02",
    )
    parser.add_argument(
        "--chunk-days",
        type=int,
        default=31,
        help="Size of each chunk in days (default: 31).",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=1.0,
        help="Seconds to sleep between API calls (default: 1.0).",
    )
    parser.add_argument(
        "--indicator",
        type=int,
        choices=list(INDICATORS.keys()),
        default=EsiosConfig().spot_indicator_id,
        help=(
            "ESIOS indicator ID to backfill "
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
    init_db(indicator_id)

    start = datetime.fromisoformat(args.start).replace(tzinfo=timezone.utc)
    end = datetime.fromisoformat(args.end).replace(tzinfo=timezone.utc)

    current = start
    while current < end:
        chunk_end = min(current + timedelta(days=args.chunk_days), end)

        start_iso = current.isoformat().replace("+00:00", "Z")
        end_iso = chunk_end.isoformat().replace("+00:00", "Z")

        print(
            f"Fetching indicator {indicator_id} "
            f"from {start_iso} to {end_iso}..."
        )

        try:
            data = get_indicator_data(indicator_id, start=start_iso, end=end_iso)
        except HTTPError as e:
            print(f"  Skipping chunk {start_iso} -> {end_iso} due to HTTP error: {e}")
            current = chunk_end
            time.sleep(args.sleep)
            continue

        values = data["indicator"]["values"]
        df = transform_indicator_values(values)

        if df.empty:
            print("  No data returned for this chunk.")
        else:
            insert_prices(df, indicator_id)
            print(f"  Stored {len(df)} rows for this chunk.")

        current = chunk_end
        time.sleep(args.sleep)

    print("Backfill complete.")


if __name__ == "__main__":
    main()


