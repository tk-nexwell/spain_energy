import os
import pandas as pd
import matplotlib.pyplot as plt

# Folder containing .1 files
input_folder = "DayAheadMarketFiles"
output_file = "combined_data.csv"

# Initialize an empty DataFrame
all_data = pd.DataFrame()

# Process each file
for file_name in os.listdir(input_folder):
    if file_name.endswith(".1"):
        print(f"Processing file: {file_name}")
        file_path = os.path.join(input_folder, file_name)
        
        # Skip the first line (title) and last line (asterisk)
        with open(file_path, 'r') as f:
            lines = f.readlines()[1:-1]  # Skip first and last line

        # Process lines and clean data
        clean_lines = []
        for line in lines:
            # Handle trailing semicolons and split the row
            columns = [value.strip() for value in line.strip(";").split(";")]

            # Check for rows with 7 columns and trim the last empty column
            if len(columns) == 7:
                columns = columns[:6]  # Keep only the first 6 columns
                clean_lines.append(columns)

        # Log the number of valid rows
        print(f"Valid rows in {file_name}: {len(clean_lines)}")

        # Load data into a DataFrame if rows are valid
        if clean_lines:
            file_data = pd.DataFrame(
                clean_lines,
                columns=["Year", "Month", "Day", "Hour", "Price1", "Price2"]
            )
            
            # Convert data types
            file_data = file_data.astype({
                "Year": int, "Month": int, "Day": int, "Hour": int,
                "Price1": float, "Price2": float
            })

            print(f"Appending {len(file_data)} rows from {file_name}")
            
            # Add the file's data to the combined DataFrame
            all_data = pd.concat([all_data, file_data], ignore_index=True)

# Check if any data was appended
if all_data.empty:
    print("No valid data found in the .1 files.")
else:
    # Save combined data to a CSV file
    all_data.to_csv(output_file, index=False)
    print(f"Data combined and saved to {output_file}")

# Analysis: Calculate Daily Average Prices
if not all_data.empty:
    all_data["Date"] = pd.to_datetime(all_data[["Year", "Month", "Day"]])
    daily_avg = all_data.groupby("Date")["Price1"].mean()

    # Plot Daily Average Prices
    plt.figure(figsize=(10, 6))
    daily_avg.plot(title="Daily Average Electricity Prices", xlabel="Date", ylabel="Price (€)", legend=False)
    plt.grid()
    plt.savefig("daily_avg_prices.png")
    plt.show()
