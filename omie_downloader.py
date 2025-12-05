"""
OMIE Day-Ahead Marginal Price Data Downloader

Downloads OMIE marginalpdbc files (daily .1 files or yearly .zip archives)
from the OMIE file-access-list HTML index. Never guesses URLs - always extracts
them from the HTML index.
"""
import os
import re
import zipfile
from pathlib import Path
from typing import Dict, Optional
from datetime import datetime, timedelta

import requests
from bs4 import BeautifulSoup


# Base URL for OMIE file access list
OMIE_INDEX_URL = (
    "https://www.omie.es/en/file-access-list?"
    "parents=/Day-ahead%20Market/1.%20Prices&"
    "dir=Day-ahead%20market%20hourly%20prices%20in%20Spain&"
    "realdir=marginalpdbc"
)

# Base URL for OMIE website
OMIE_BASE_URL = "https://www.omie.es"

# Local storage directory
DATA_DIR = Path("data") / "omie"
DATA_DIR.mkdir(parents=True, exist_ok=True)


def get_file_index() -> Dict[str, str]:
    """
    Load the OMIE file-access-list page and extract all file links.
    
    Returns:
        Dictionary mapping filename -> full download URL
        Example: {"marginalpdbc_20250101.1": "https://www.omie.es/.../file.1"}
    """
    print("Loading OMIE file index...")
    
    try:
        response = requests.get(OMIE_INDEX_URL, timeout=30)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Error loading OMIE index: {e}")
        return {}
    
    soup = BeautifulSoup(response.text, "html.parser")
    file_index = {}
    
    # Find all links on the page
    links = soup.find_all("a", href=True)
    
    for link in links:
        href = link.get("href", "")
        text = link.get_text(strip=True)
        
        # Look for marginalpdbc files (.1 or .zip)
        if "marginalpdbc" in text.lower() and (text.endswith(".1") or text.endswith(".zip")):
            filename = text.strip()
            
            # Construct full URL
            if href.startswith("http"):
                full_url = href
            elif href.startswith("/"):
                full_url = OMIE_BASE_URL + href
            else:
                full_url = OMIE_BASE_URL + "/" + href
            
            file_index[filename] = full_url
    
    print(f"Found {len(file_index)} files in OMIE index")
    return file_index


def download_file(url: str, local_path: Path, force: bool = False) -> bool:
    """
    Download a file from URL to local path.
    
    Args:
        url: Full download URL
        local_path: Local file path to save to
        force: If True, re-download even if file exists
    
    Returns:
        True if download succeeded, False otherwise
    """
    # Skip if file already exists (unless force=True)
    if local_path.exists() and not force:
        print(f"  File already exists: {local_path.name}")
        return True
    
    try:
        print(f"  Downloading: {local_path.name}...", end=" ", flush=True)
        response = requests.get(url, timeout=60, stream=True)
        response.raise_for_status()
        
        # Write file
        with open(local_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        print("✓")
        return True
    except requests.RequestException as e:
        print(f"✗ Error: {e}")
        return False


def download_daily(day: str, file_index: Optional[Dict[str, str]] = None, force: bool = False) -> Optional[Path]:
    """
    Download a single daily .1 file for a given date.
    
    Args:
        day: Date string in format "YYYYMMDD" (e.g., "20230101")
        file_index: Optional pre-loaded file index. If None, will fetch it.
        force: If True, re-download even if file exists
    
    Returns:
        Path to downloaded file, or None if download failed
    """
    if file_index is None:
        file_index = get_file_index()
    
    filename = f"marginalpdbc_{day}.1"
    
    if filename not in file_index:
        print(f"  File not found in OMIE index: {filename}")
        return None
    
    url = file_index[filename]
    local_path = DATA_DIR / filename
    
    if download_file(url, local_path, force=force):
        return local_path
    return None


def download_year(year: str, file_index: Optional[Dict[str, str]] = None, force: bool = False) -> int:
    """
    Download the yearly .zip archive for a given year and extract all .1 files.
    
    Args:
        year: Year string (e.g., "2022")
        file_index: Optional pre-loaded file index. If None, will fetch it.
        force: If True, re-download even if file exists
    
    Returns:
        Number of .1 files extracted
    """
    if file_index is None:
        file_index = get_file_index()
    
    zip_filename = f"marginalpdbc_{year}.zip"
    
    if zip_filename not in file_index:
        print(f"  Yearly ZIP not found in OMIE index: {zip_filename}")
        return 0
    
    url = file_index[zip_filename]
    zip_path = DATA_DIR / zip_filename
    
    # Download ZIP file
    if not download_file(url, zip_path, force=force):
        return 0
    
    # Extract ZIP file
    extract_dir = DATA_DIR / f"marginalpdbc_{year}"
    extract_dir.mkdir(exist_ok=True)
    
    extracted_count = 0
    try:
        print(f"  Extracting {zip_filename}...", end=" ", flush=True)
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            # Extract all .1 files
            for member in zip_ref.namelist():
                if member.endswith(".1"):
                    # Extract to year-specific folder
                    member_path = Path(member)
                    extract_path = extract_dir / member_path.name
                    
                    # Skip if already extracted (unless force)
                    if extract_path.exists() and not force:
                        continue
                    
                    zip_ref.extract(member, extract_dir)
                    extracted_count += 1
        
        print(f"✓ ({extracted_count} files)")
        return extracted_count
    except zipfile.BadZipFile:
        print("✗ Invalid ZIP file")
        return 0
    except Exception as e:
        print(f"✗ Error extracting: {e}")
        return 0


def download_range(start_date: str, end_date: str, force: bool = False) -> Dict[str, int]:
    """
    Download OMIE files for a date range, automatically choosing between
    daily .1 files and yearly .zip archives.
    
    Downloads are done from most recent to oldest (so ZIP files are hit at the end).
    
    Args:
        start_date: Start date in format "YYYYMMDD"
        end_date: End date in format "YYYYMMDD"
        force: If True, re-download even if files exist
    
    Returns:
        Dictionary with download statistics:
        {
            "daily_files": number of daily files downloaded,
            "yearly_zips": number of yearly ZIPs downloaded,
            "total_extracted": total .1 files extracted from ZIPs
        }
    """
    # Parse dates
    start_dt = datetime.strptime(start_date, "%Y%m%d")
    end_dt = datetime.strptime(end_date, "%Y%m%d")
    
    # Get file index once
    print("Loading OMIE file index...")
    file_index = get_file_index()
    
    if not file_index:
        print("Error: Could not load file index")
        return {"daily_files": 0, "yearly_zips": 0, "total_extracted": 0}
    
    # Group dates by year
    dates_by_year = {}
    current_date = end_dt  # Start from most recent
    
    while current_date >= start_dt:
        year = current_date.strftime("%Y")
        day = current_date.strftime("%Y%m%d")
        
        if year not in dates_by_year:
            dates_by_year[year] = []
        dates_by_year[year].append(day)
        
        current_date -= timedelta(days=1)
    
    stats = {
        "daily_files": 0,
        "yearly_zips": 0,
        "total_extracted": 0,
    }
    
    # Process years from most recent to oldest
    for year in sorted(dates_by_year.keys(), reverse=True):
        days = sorted(dates_by_year[year], reverse=True)  # Most recent first within year
        
        # Check if yearly ZIP exists in index
        zip_filename = f"marginalpdbc_{year}.zip"
        has_zip = zip_filename in file_index
        
        if has_zip:
            # Download yearly ZIP
            print(f"\nProcessing year {year} (ZIP archive)...")
            extracted = download_year(year, file_index=file_index, force=force)
            stats["yearly_zips"] += 1
            stats["total_extracted"] += extracted
        else:
            # Download individual daily files
            print(f"\nProcessing year {year} (daily files)...")
            for day in days:
                result = download_daily(day, file_index=file_index, force=force)
                if result:
                    stats["daily_files"] += 1
    
    print(f"\n{'='*50}")
    print("Download Summary:")
    print(f"  Daily files downloaded: {stats['daily_files']}")
    print(f"  Yearly ZIPs downloaded: {stats['yearly_zips']}")
    print(f"  Total files extracted from ZIPs: {stats['total_extracted']}")
    print(f"{'='*50}")
    
    return stats


def list_downloaded_files() -> Dict[str, list]:
    """
    List all downloaded OMIE files in the data directory.
    
    Returns:
        Dictionary with:
        {
            "daily_files": list of .1 file paths,
            "extracted_files": list of extracted .1 file paths (from ZIPs)
        }
    """
    daily_files = []
    extracted_files = []
    
    # Find all .1 files directly in DATA_DIR
    for file_path in DATA_DIR.glob("*.1"):
        daily_files.append(file_path)
    
    # Find all .1 files in year subdirectories
    for year_dir in DATA_DIR.glob("marginalpdbc_*"):
        if year_dir.is_dir():
            for file_path in year_dir.glob("*.1"):
                extracted_files.append(file_path)
    
    return {
        "daily_files": sorted(daily_files),
        "extracted_files": sorted(extracted_files),
    }


if __name__ == "__main__":
    # Example usage
    
    # Download a single daily file
    print("Example 1: Download single daily file")
    print("-" * 50)
    download_daily("20230101")
    
    # Download a yearly ZIP
    print("\nExample 2: Download yearly ZIP archive")
    print("-" * 50)
    download_year("2022")
    
    # Download a date range (most recent to oldest)
    print("\nExample 3: Download date range (backfill)")
    print("-" * 50)
    download_range("20240101", "20241231")
    
    # List downloaded files
    print("\nExample 4: List downloaded files")
    print("-" * 50)
    files = list_downloaded_files()
    print(f"Daily files: {len(files['daily_files'])}")
    print(f"Extracted files: {len(files['extracted_files'])}")

