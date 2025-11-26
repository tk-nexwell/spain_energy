import requests
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta

# 1. Set your token
API_TOKEN = "YOUR_ESIOS_TOKEN"

headers = {
    "Host": "api.esios.ree.es",
    "x-api-key": API_TOKEN
}

# 2. Select indicator ID (you’ll need to look it up)
indicator_id = 1234  # replace with correct ID for 15-min price data

# 3. Define time range (prior day)
yesterday = datetime.utcnow().date() - timedelta(days=1)
start_date = f"{yesterday}T00:00:00"
end_date   = f"{yesterday}T23:59:59"

url = f"https://api.esios.ree.es/indicators/{indicator_id}"
params = {
    "start_date": start_date,
    "end_date":   end_date,
    "time_trunc": "15min"
}

res = requests.get(url, headers=headers, params=params)
res.raise_for_status()
data = res.json()["indicator"]["values"]

# 4. Load into DataFrame
df = pd.DataFrame(data)
df["datetime"] = pd.to_datetime(df["datetime_utc"])
df = df.set_index("datetime")
df = df.sort_index()

# 5. Plot
plt.figure(figsize=(12,6))
plt.plot(df.index, df["value"], marker=".", linestyle="-")
plt.title(f"Spain 15-min Electricity Indicator {indicator_id} on {yesterday}")
plt.xlabel("Time")
plt.ylabel("Value (€ / MWh or other unit)")
plt.grid(True)
plt.show()
