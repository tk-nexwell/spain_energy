import pandas as pd
import matplotlib.pyplot as plt

# File containing the combined data
input_file = "combined_data.csv"
output_image = "daily_avg_prices_updated.png"

# Load the combined dataset
try:
    data = pd.read_csv(input_file)
    print(f"Loaded data with {len(data)} rows.")

    # Ensure the "Date" column is in datetime format
    data["Date"] = pd.to_datetime(data[["Year", "Month", "Day"]])

    # Calculate daily average prices
    daily_avg = data.groupby("Date")["Price1"].mean()

    # Plot the daily average prices
    plt.figure(figsize=(12, 6))
    daily_avg.plot(
        title="Daily Average Electricity Prices (2019–2025)",
        xlabel="Date",
        ylabel="Price (€)",
        legend=False
    )
    plt.grid()
    plt.savefig(output_image)
    plt.show()

    print(f"Updated plot saved as {output_image}.")
except FileNotFoundError:
    print(f"File {input_file} not found. Please ensure the combined dataset exists.")
except Exception as e:
    print(f"An error occurred: {e}")
