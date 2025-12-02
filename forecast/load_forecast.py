import pandas as pd
import sqlite3
from pathlib import Path
from pyxlsb import open_workbook

def insert_forecast(df: pd.DataFrame, db_path: str):
    """
    Inserts forecast price data into SQLite, automatically handling
    simple (Baringa) vs detailed (Aurora) schemas.
    """

    # Ensure directory exists
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Aurora uses additional metadata columns
    is_detailed = all(col in df.columns for col in ["market", "country", "unit"])

    if is_detailed:
        # Full table for Aurora
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS forecast_prices (
                datetime TEXT,
                price_eur_per_mwh REAL,
                provider TEXT,
                scenario TEXT,
                version TEXT,
                market TEXT,
                country TEXT,
                unit TEXT,
                PRIMARY KEY (datetime, provider, scenario, version, market, country)
            );
        """)
        insert_cols = [
            "datetime",
            "price_eur_per_mwh",
            "provider",
            "scenario",
            "version",
            "market",
            "country",
            "unit"
        ]

    else:
        # Simple table for Baringa
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS forecast_prices (
                datetime TEXT,
                price_eur_per_mwh REAL,
                provider TEXT,
                scenario TEXT,
                version TEXT,
                PRIMARY KEY (datetime, provider, scenario, version)
            );
        """)
        insert_cols = [
            "datetime",
            "price_eur_per_mwh",
            "provider",
            "scenario",
            "version"
        ]

    # Convert datetime to string for SQLite
    df["datetime"] = df["datetime"].astype(str)

    # Prepare rows
    data = list(df[insert_cols].itertuples(index=False, name=None))

    placeholders = ", ".join(["?"] * len(insert_cols))

    cursor.executemany(
        f"""
        INSERT OR REPLACE INTO forecast_prices
        ({', '.join(insert_cols)})
        VALUES ({placeholders})
        """,
        data
    )

    conn.commit()
    conn.close()

    print(f"✅ Inserted {len(df)} rows into forecast_prices.")

def load_baringa_forecast(file_path: Path, db_path="data/energy_data.db", version="2025_Q2"):

    print(f"Loading Baringa file: {file_path.name}")
    # Read 'ES' sheet starting from row 10 (zero-indexed skiprows=9), columns B to I
    df = pd.read_excel(
        file_path,
        sheet_name="ES",
        skiprows=9,
        usecols="B:I",
        engine="openpyxl"
    )
    df.columns = ["Year", "Month", "Day", "Hour", "Reference", "LowCommod", "NetZero", "NetZeroHigh"]
    print(df.head())
    print(df.columns)

    # Create datetime
    df["datetime"] = pd.to_datetime(df[["Year", "Month", "Day"]]) + pd.to_timedelta(df["Hour"], unit="h")

    # Melt scenario columns to long format
    df_melted = df.melt(
        id_vars=["datetime"],
        value_vars=["Reference", "LowCommod", "NetZero", "NetZeroHigh"],
        var_name="scenario",
        value_name="price_eur_per_mwh"
    )

    df_melted["provider"] = "Baringa"
    df_melted["version"] = version

    insert_forecast(df_melted, db_path)

def match_sheet(sheet_names, keywords):
    for name in sheet_names:
        if all(k.lower() in name.lower() for k in keywords):
            return name
    return None


def load_aurora_forecast(file_path: Path, db_path="data/energy_data.db", version="Jun25"):

    # discover sheet names
    with open_workbook(file_path) as wb:
        sheet_names = list(wb.sheets)

    # fuzzy match Aurora sheets
    central_sheet = match_sheet(sheet_names, ["hourly", "central"])
    low_sheet     = match_sheet(sheet_names, ["hourly", "low"])

    scenarios = {
        central_sheet: "central",
        low_sheet: "low"
    }

    all_records = []

    for sheet_name, scenario in scenarios.items():

        if sheet_name is None:
            print(f"⚠️ Sheet for scenario '{scenario}' not found, skipping.")
            continue

        print(f"  → Processing sheet: {sheet_name}")

        df = pd.read_excel(
            file_path,
            sheet_name=sheet_name,
            header=[4, 5, 6],
            skiprows=7,
            engine="pyxlsb"
        )

        base_cols = ['Year', 'Month', 'Day', 'Hour']
        df['datetime'] = pd.to_datetime(df[base_cols]) + pd.to_timedelta(df['Hour'], unit='h')

        value_cols = [col for col in df.columns if col[0] not in base_cols]

        df_long = df.melt(id_vars=["datetime"], value_vars=value_cols,
                          var_name=["market", "country", "unit"],
                          value_name="price_eur_per_mwh")

        df_long["provider"] = "Aurora"
        df_long["scenario"] = scenario
        df_long["version"] = version

        all_records.append(df_long)

    if not all_records:
        print("❌ No valid Aurora sheets found. Nothing loaded.")
        return

    final_df = pd.concat(all_records).dropna(subset=["price_eur_per_mwh"])

    insert_forecast(final_df, db_path)
    print(f"✅ Loaded Aurora version {version} into DB.")
