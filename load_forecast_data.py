from forecast.load_forecast import load_baringa_forecast, load_aurora_forecast, insert_forecast
from pathlib import Path

if __name__ == "__main__":
    forecasts_dir = Path("forecasts")

    load_baringa_forecast(forecasts_dir / "Baringa_Spain_Wholesale_Power_Market_Results_2025_Q2_Hourly_Results_v1_0.xlsx")
    load_aurora_forecast(forecasts_dir / "Aurora_Jun25_IBE_Flexible_Energy_Market_Granular_Data.xlsb")
