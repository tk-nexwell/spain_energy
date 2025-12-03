## Simple ESIOS Fetcher

This is a minimal Python project to fetch spot prices from the ESIOS API and save them as CSV files.

### Setup

1. Create and activate a virtual environment (optional but recommended).
2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Create a `.env` file in the project root with your ESIOS API token:

```bash
ESIOS_API_TOKEN=your_token_here
```

### Usage

Fetch day-ahead spot prices (indicator 600) for a date range and save to CSV:

```bash
python fetch_spot_to_csv.py --start 2025-01-01 --end 2025-12-31 --out esios_spot_2025.csv
```

If you omit `--start` and `--end`, the script defaults to year-to-date for the current year.


