"""
Standardize datetime format in historical_prices table to "YYYY-MM-DD HH:MM:SS".

Converts all datetime values from various formats (e.g., "2025-10-10T23:45:00Z")
to the standard format "2025-10-10 23:45:00".
"""
import sqlite3
from datetime import datetime
from pathlib import Path

from db import DB_PATH


def standardize_datetime_string(ts_str: str) -> str:
    """Convert a datetime string to standardized format."""
    if ts_str is None:
        return ts_str
    
    original = str(ts_str)
    
    # Handle different formats
    normalized = original
    if "T" in normalized:
        normalized = normalized.replace("T", " ")
    if normalized.endswith("Z"):
        normalized = normalized[:-1]
    if "+" in normalized:
        # Remove timezone offset (e.g., "+01:00")
        normalized = normalized.split("+")[0]
    if "-" in normalized and len(normalized.split("-")) > 3:
        # Remove timezone offset (e.g., "-05:00")
        parts = normalized.split("-")
        if len(parts) > 3:
            normalized = "-".join(parts[:3]) + " " + parts[-1].split(" ")[-1] if " " in parts[-1] else normalized
    
    # Parse and reformat to standard format
    try:
        formats = [
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%d %H:%M:%S.%f",
            "%Y-%m-%dT%H:%M:%S.%fZ",
        ]
        
        dt = None
        for fmt in formats:
            try:
                dt = datetime.strptime(normalized, fmt)
                break
            except ValueError:
                continue
        
        if dt is None:
            # Try using fromisoformat as fallback
            dt = datetime.fromisoformat(normalized.replace("Z", "+00:00"))
            if dt.tzinfo:
                dt = dt.replace(tzinfo=None)  # Remove timezone info
        
        # Format to standard: "YYYY-MM-DD HH:MM:SS"
        standardized = dt.strftime("%Y-%m-%d %H:%M:%S")
        return standardized
    except Exception:
        return original  # Return original if parsing fails


def standardize_datetime_format():
    """Standardize all datetime values in historical_prices table using bulk operations."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    
    print("Step 1: Getting all unique datetime values...")
    cur.execute("SELECT DISTINCT datetime FROM historical_prices WHERE datetime IS NOT NULL")
    all_datetimes = [row[0] for row in cur.fetchall()]
    
    print(f"Found {len(all_datetimes)} unique datetime values to standardize")
    
    print("Step 2: Creating datetime mapping...")
    # Create mapping of old -> new datetime
    datetime_map = {}
    examples = []
    
    for dt_str in all_datetimes:
        original = str(dt_str)
        standardized = standardize_datetime_string(original)
        
        if standardized != original:
            datetime_map[original] = standardized
            if len(examples) < 10:
                examples.append((original, standardized))
    
    if not datetime_map:
        print("✓ All datetime values are already in standardized format")
        conn.close()
        return
    
    print(f"Step 3: Found {len(datetime_map)} datetime values that need updating")
    print("Examples:")
    for orig, std in examples:
        print(f"  {orig} → {std}")
    
    print("Step 4: Creating temporary mapping table...")
    # Create temporary table for mapping
    cur.execute("""
        CREATE TEMPORARY TABLE datetime_mapping (
            old_datetime TEXT PRIMARY KEY,
            new_datetime TEXT
        )
    """)
    
    # Insert mappings in bulk
    cur.executemany(
        "INSERT INTO datetime_mapping (old_datetime, new_datetime) VALUES (?, ?)",
        datetime_map.items()
    )
    
    print("Step 5: Updating historical_prices table in bulk...")
    # Update all rows using the mapping table
    cur.execute("""
        UPDATE historical_prices
        SET datetime = (
            SELECT new_datetime 
            FROM datetime_mapping 
            WHERE datetime_mapping.old_datetime = historical_prices.datetime
        )
        WHERE datetime IN (SELECT old_datetime FROM datetime_mapping)
    """)
    
    updated_rows = cur.rowcount
    conn.commit()
    
    # Clean up temporary table
    cur.execute("DROP TABLE IF EXISTS datetime_mapping")
    conn.close()
    
    print(f"\n✓ Standardization complete:")
    print(f"  Updated {updated_rows} rows with {len(datetime_map)} unique datetime values")


if __name__ == "__main__":
    standardize_datetime_format()

