import pandas as pd

# File containing the combined data
input_file = "combined_data.csv"

try:
    # Load the combined dataset
    data = pd.read_csv(input_file)
    print(f"Loaded data with {len(data)} rows.")

    # Ensure the "Date" column is in datetime format
    data["Date"] = pd.to_datetime(data[["Year", "Month", "Day"]])

    # 1) Number of hours at 0 or less per year
    zero_or_less = data[data["Price1"] <= 0]
    zero_or_less_counts = zero_or_less.groupby("Year").size()
    print("\nNumber of hours at 0 or less per year:")
    print(zero_or_less_counts)

    # 2) Time of day when 0 or less phenomenon is most likely to happen
    most_likely_hour = zero_or_less["Hour"].value_counts().idxmax()
    print(f"\nTime of day when 0 or less is most likely to happen: {most_likely_hour}:00")

    # 3) Average daily spread (min-to-max difference) per year
    daily_spread = data.groupby("Date").agg(
        daily_min=("Price1", "min"),
        daily_max=("Price1", "max")
    )
    daily_spread["daily_diff"] = daily_spread["daily_max"] - daily_spread["daily_min"]
    avg_spread_per_year = daily_spread.groupby(daily_spread.index.year)["daily_diff"].mean()
    print("\nAverage daily spread between min and max per year:")
    print(avg_spread_per_year)

except FileNotFoundError:
    print(f"File {input_file} not found. Please ensure the combined dataset exists.")
except Exception as e:
    print(f"An error occurred: {e}")
