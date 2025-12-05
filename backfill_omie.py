"""
Backfill OMIE day-ahead prices from 2018 to today.

Uses omie_downloader.py to download files (supports both daily .1 files
and yearly .zip archives), parses them, and stores prices in the
historical_prices table under the OMIE_DA_prices column.
"""
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
import time

import pandas as pd

from db import DB_PATH, DATA_DIR
from omie_downloader import download_range, get_file_index, DATA_DIR as OMIE_DATA_DIR


def find_omie_file(day: str) -> Path | None:
    """
    Find an OMIE .1 file for a given date.
    Checks both direct downloads and extracted ZIP files.
    
    Args:
        day: Date string in format YYYYMMDD
    
    Returns:
        Path to the .1 file, or None if not found
    """
    year = day[:4]
    filename = f"marginalpdbc_{day}.1"
    
    # Check direct downloads
    direct_path = OMIE_DATA_DIR / filename
    if direct_path.exists():
        return direct_path
    
    # Check extracted ZIP files
    zip_dir = OMIE_DATA_DIR / f"marginalpdbc_{year}"
    if zip_dir.exists():
        zip_path = zip_dir / filename
        if zip_path.exists():
            return zip_path
    
    return None


def parse_omie_file(file_path: Path) -> pd.DataFrame:
    """
    Parse an OMIE .1 file and return a DataFrame with prices.
    
    File format:
    - Header: MARGINALPDBC;
    - Data rows: Year;Month;Day;Period;Price1;Price2;
    - Footer: *
    
    Price1 = Spain (OMIE_SP_DA_prices)
    Price2 = Portugal (OMIE_PT_DA_prices)
    
    For hourly data (before Oct 2025): Period is 1-24 (hours)
    For 15-minute data (Oct 2025+): Period is 1-96 (15-minute intervals)
    Some files may have more than 96 periods (e.g., 100) - handle dynamically
    
    Returns:
        DataFrame with columns: datetime, year, month, day, hour, minute, 
        OMIE_SP_DA_prices, OMIE_PT_DA_prices
        Datetime format: "YYYY-MM-DD HH:MM:SS" (standardized format)
    """
    rows = []
    
    with open(file_path, "r", encoding="latin-1") as f:
        all_lines = [line.strip() for line in f if line.strip() and line.strip() != "MARGINALPDBC;" and line.strip() != "*"]
    
    # Detect resolution by checking the maximum period number
    # If max period > 24, it's 15-minute data
    max_period = 0
    for line in all_lines:
        parts = line.split(";")
        if len(parts) >= 4:
            try:
                period = int(parts[3])
                max_period = max(max_period, period)
            except (ValueError, IndexError):
                continue
    
    # Determine if 15-minute data: if max period > 24, it's 15-minute
    is_15min_data = max_period > 24
    
    # For 15-minute data, always use 4 intervals per hour (15-minute intervals)
    # Some files may have extra periods beyond 96, which we'll skip
    if is_15min_data:
        intervals_per_hour = 4  # Always 4 for 15-minute data (00, 15, 30, 45)
    else:
        intervals_per_hour = 1  # Hourly data
    
    for line in all_lines:
        # Parse data row: Year;Month;Day;Period;Price1;Price2;
        parts = line.split(";")
        if len(parts) >= 6:  # Need at least 6 parts for both prices
            try:
                year = int(parts[0])
                month = int(parts[1])
                day = int(parts[2])
                period = int(parts[3])
                # Price1 = Spain, Price2 = Portugal
                price_sp = float(parts[4]) if parts[4] else None
                price_pt = float(parts[5]) if len(parts) > 5 and parts[5] else None
                
                if price_sp is not None:  # At least Spain price must exist
                    if is_15min_data:
                        # 15-minute data: period is 1 to max_period (usually 96, but can be more)
                        # Convert period to hour and minute
                        # Period 1 = 00:00, Period 2 = 00:15, ..., Period 96 = 23:45
                        period_0_indexed = period - 1  # Convert to 0-based
                        hour = period_0_indexed // 4  # 4 intervals per hour (0-23)
                        minute = (period_0_indexed % 4) * 15  # 0, 15, 30, 45
                        
                        # Skip periods that would result in invalid hours (> 23)
                        # This handles files with periods beyond 96 (e.g., 97-100)
                        if hour > 23:
                            continue  # Skip periods beyond 24 hours
                    else:
                        # Hourly data: period is 1-24 (hours)
                        # Convert to 0-23 hour format
                        # Period 1 = hour 0, Period 24 = hour 23
                        hour = period - 1 if period <= 24 else period - 1
                        if hour < 0 or hour > 23:
                            continue  # Skip invalid hours
                        minute = 0
                    
                    # Create datetime string in standardized format: "YYYY-MM-DD HH:MM:SS"
                    dt = datetime(year, month, day, hour, minute)
                    datetime_str = dt.strftime("%Y-%m-%d %H:%M:%S")
                    
                    rows.append({
                        "datetime": datetime_str,
                        "year": year,
                        "month": month,
                        "day": day,
                        "hour": hour,
                        "minute": minute,
                        "OMIE_SP_DA_prices": price_sp,
                        "OMIE_PT_DA_prices": price_pt,
                    })
            except (ValueError, IndexError, OverflowError) as e:
                # Silently skip invalid lines instead of printing warnings for every one
                continue
    
    if not rows:
        return pd.DataFrame()
    
    return pd.DataFrame(rows)


def ensure_omie_columns_exist():
    """Add OMIE_SP_DA_prices and OMIE_PT_DA_prices columns to historical_prices table if they don't exist."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    
    # Check existing columns
    cur.execute("PRAGMA table_info(historical_prices)")
    columns = [col[1] for col in cur.fetchall()]
    
    # Add Spain column
    if "OMIE_SP_DA_prices" not in columns:
        print("Adding OMIE_SP_DA_prices column to historical_prices table...")
        cur.execute("ALTER TABLE historical_prices ADD COLUMN OMIE_SP_DA_prices REAL")
        conn.commit()
        print("✓ OMIE_SP_DA_prices column added")
    else:
        print("OMIE_SP_DA_prices column already exists")
    
    # Add Portugal column
    if "OMIE_PT_DA_prices" not in columns:
        print("Adding OMIE_PT_DA_prices column to historical_prices table...")
        cur.execute("ALTER TABLE historical_prices ADD COLUMN OMIE_PT_DA_prices REAL")
        conn.commit()
        print("✓ OMIE_PT_DA_prices column added")
    else:
        print("OMIE_PT_DA_prices column already exists")
    
    # Migrate old OMIE_DA_prices to OMIE_SP_DA_prices if it exists
    if "OMIE_DA_prices" in columns and "OMIE_SP_DA_prices" in columns:
        print("Migrating OMIE_DA_prices to OMIE_SP_DA_prices...")
        cur.execute(
            """
            UPDATE historical_prices 
            SET OMIE_SP_DA_prices = OMIE_DA_prices 
            WHERE OMIE_SP_DA_prices IS NULL AND OMIE_DA_prices IS NOT NULL
            """
        )
        migrated = cur.rowcount
        conn.commit()
        if migrated > 0:
            print(f"✓ Migrated {migrated} rows from OMIE_DA_prices to OMIE_SP_DA_prices")
    
    conn.close()


def insert_omie_prices(df: pd.DataFrame) -> int:
    """
    Insert or update OMIE prices in the historical_prices table.
    
    Updates existing rows or inserts new ones.
    Updates both OMIE_SP_DA_prices and OMIE_PT_DA_prices columns, leaving other columns unchanged.
    
    Returns:
        Number of rows inserted/updated
    """
    if df.empty:
        return 0
    
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    
    updated_count = 0
    inserted_count = 0
    
    for _, row in df.iterrows():
        # Check if row exists
        cur.execute(
            "SELECT datetime FROM historical_prices WHERE datetime = ?",
            (row["datetime"],)
        )
        exists = cur.fetchone() is not None
        
        if exists:
            # Update existing row
            cur.execute(
                """
                UPDATE historical_prices 
                SET OMIE_SP_DA_prices = ?, OMIE_PT_DA_prices = ? 
                WHERE datetime = ?
                """,
                (
                    row.get("OMIE_SP_DA_prices"),
                    row.get("OMIE_PT_DA_prices"),
                    row["datetime"]
                )
            )
            updated_count += 1
        else:
            # Insert new row (need all required columns)
            cur.execute(
                """
                INSERT INTO historical_prices
                (datetime, year, month, day, hour, minute, ESIOS_600_DA_prices, OMIE_SP_DA_prices, OMIE_PT_DA_prices)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row["datetime"],
                    row["year"],
                    row["month"],
                    row["day"],
                    row["hour"],
                    row["minute"],
                    None,  # ESIOS_600_DA_prices
                    row.get("OMIE_SP_DA_prices"),
                    row.get("OMIE_PT_DA_prices"),
                )
            )
            inserted_count += 1
    
    conn.commit()
    conn.close()
    
    return updated_count + inserted_count


def get_existing_omie_dates() -> set[str]:
    """
    Get set of dates (YYYYMMDD) that already have OMIE data.
    Excludes dates from October 2025 onwards (need re-upload with 15-minute parsing).
    """
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    
    try:
        # Only get dates before October 2025 (hourly data, already correct)
        # Check for either OMIE_SP_DA_prices or OMIE_DA_prices (for backward compatibility)
        cur.execute(
            """
            SELECT DISTINCT year || '-' || printf('%02d', month) || '-' || printf('%02d', day) as date_str 
            FROM historical_prices 
            WHERE (OMIE_SP_DA_prices IS NOT NULL OR OMIE_DA_prices IS NOT NULL)
            AND (year < 2025 OR (year = 2025 AND month < 10))
            """
        )
        dates = {row[0].replace("-", "") for row in cur.fetchall()}
    except sqlite3.OperationalError:
        dates = set()
    
    conn.close()
    return dates


def delete_omie_from_oct_2025():
    """Delete OMIE_SP_DA_prices and OMIE_PT_DA_prices data from October 1, 2025 onwards to allow re-upload."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    
    cutoff_date = "2025-10-01 00:00:00"
    cur.execute(
        """
        UPDATE historical_prices 
        SET OMIE_SP_DA_prices = NULL, OMIE_PT_DA_prices = NULL 
        WHERE datetime >= ?
        """,
        (cutoff_date,)
    )
    
    rows_affected = cur.rowcount
    conn.commit()
    conn.close()
    
    if rows_affected > 0:
        print(f"✓ Deleted OMIE data from {rows_affected} rows (from {cutoff_date} onwards)")
        print("  These dates will be re-uploaded with correct 15-minute parsing and both Spain/Portugal prices")


def main():
    """Backfill OMIE prices from 2018 to today."""
    print("OMIE Price Backfill")
    print("=" * 50)
    
    # Ensure columns exist
    ensure_omie_columns_exist()
    
    # Delete incorrect 15-minute data from Oct 2025 onwards
    print("\nCleaning up incorrect 15-minute data from October 2025 onwards...")
    delete_omie_from_oct_2025()
    
    # Get dates that already have data (only before Oct 2025)
    print("\nChecking existing data...")
    existing_dates = get_existing_omie_dates()
    print(f"Found {len(existing_dates)} dates with existing OMIE data (before Oct 2025)")
    
    # Generate date range from 2018-01-01 to today
    start_date = datetime(2018, 1, 1)
    end_date = datetime.now()
    
    print(f"\nStep 1: Downloading OMIE files from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}...")
    print("(This will download from most recent to oldest, using ZIP files when available)")
    print("=" * 50)
    
    # Download files using omie_downloader (most recent to oldest)
    download_stats = download_range(
        start_date.strftime("%Y%m%d"),
        end_date.strftime("%Y%m%d"),
        force=False  # Don't re-download existing files
    )
    
    print(f"\nStep 2: Parsing and inserting data into database...")
    print("=" * 50)
    
    total_parsed = 0
    total_inserted = 0
    failed_dates = []
    
    # Process all dates from start to end
    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.strftime("%Y%m%d")
        date_display = current_date.strftime("%Y-%m-%d")
        
        # Skip if we already have data for this date
        if date_str in existing_dates:
            if current_date.day == 1:  # Print progress monthly
                print(f"{date_display}: Already have data, skipping...")
            current_date += timedelta(days=1)
            continue
        
        # Find the downloaded file
        file_path = find_omie_file(date_str)
        
        if file_path is None:
            failed_dates.append(date_display)
            current_date += timedelta(days=1)
            continue
        
        # Parse file
        try:
            df = parse_omie_file(file_path)
            if df.empty:
                if current_date.day == 1:  # Print progress monthly
                    print(f"{date_display}: No data parsed")
                current_date += timedelta(days=1)
                continue
            
            total_parsed += 1
            
            # Insert into database
            rows_inserted = insert_omie_prices(df)
            total_inserted += rows_inserted
            
            if current_date.day == 1 or rows_inserted > 0:  # Print progress monthly or when data inserted
                print(f"{date_display}: Inserted {rows_inserted} rows")
            
        except Exception as e:
            print(f"{date_display}: Error parsing/inserting: {e}")
            failed_dates.append(date_display)
        
        current_date += timedelta(days=1)
    
    print("\n" + "=" * 50)
    print("Summary:")
    print(f"  Daily files downloaded: {download_stats['daily_files']}")
    print(f"  Yearly ZIPs downloaded: {download_stats['yearly_zips']}")
    print(f"  Files extracted from ZIPs: {download_stats['total_extracted']}")
    print(f"  Files parsed: {total_parsed}")
    print(f"  Total rows inserted/updated: {total_inserted}")
    if failed_dates:
        print(f"  Failed dates: {len(failed_dates)}")
        if len(failed_dates) <= 20:
            print(f"    {', '.join(failed_dates)}")
        else:
            print(f"    {', '.join(failed_dates[:20])} ... and {len(failed_dates) - 20} more")


if __name__ == "__main__":
    main()

