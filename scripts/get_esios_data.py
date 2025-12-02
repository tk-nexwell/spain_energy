import requests
import pandas as pd
import os
from dotenv import load_dotenv

# Load your API token from .env
load_dotenv()
API_TOKEN = os.getenv("ESIOS_API_TOKEN")

# Indicator ID for day-ahead price (likely correct = 1001)
INDICATOR_ID = 1001

# Define time window
start_date = "2023-11-20T00:00:00Z"
end_date = "2023-11-21T00:00:00Z"

url = f"https://api.esios.ree.es/indicators/{INDICATOR_ID}"

headers = {
    "Accept": "application/json; application/vnd.esios-api-v1+json",
    "Content-Type": "application/json",
    "Host": "api.esios.ree.es",
    "x-api-key": API_TOKEN,
    "Cache-Control": "no-cache"
}

params = {
    "start_date": start_date,
    "end_date": end_date,
    "time_trunc": "hour"
}

response = requests.get(url, headers=headers, params=params)

if response.status_code == 200:
    data = response.json()
    values = data["indicator"]["values"]
    df = pd.DataFrame(values)

    # Filter for Península only
    df = df[df["geo_name"] == "Península"].copy()

    # Parse datetime
    df["datetime"] = pd.to_datetime(df["datetime"])

    # Keep only what's useful
    df = df[["datetime", "value"]].rename(columns={"value": "price_eur_per_mwh"})

    print(df.head())

    # Optional: Save to CSV
    df.to_csv("esios_peninsula_2023-11-20.csv", index=False)
else:
    print("Error:", response.status_code)
    print(response.text)
