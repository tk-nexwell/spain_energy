import os
import pandas as pd

# Define the existing combined file and the new folders
existing_combined_file = "combined_data.csv"
new_folders = ["marginalpdbc_2019", "marginalpdbc_2020", "marginalpdbc_2021", "marginalpdbc_2022"]

# Load the existing combined dataset
if os.path.exists(existing_combined_file):
    combined_data = pd.read_csv(existing_combined_file)
    print(f"Loaded existing combined data with {len(combined_data)} rows.")
else:
    print("Existing combined file not found. Creating a new combined dataset.")
    combined_data = pd.DataFrame()

# Process each new folder
for folder in new_folders:
    print(f"Processing folder: {folder}")
    for file_name in os.listdir(folder):
        if file_name.endswith(".1"):
            file_path = os.path.join(folder, file_name)
            
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
                combined_data = pd.concat([combined_data, file_data], ignore_index=True)

# Save the updated combined data to the same file
combined_data.to_csv(existing_combined_file, index=False)
print(f"Updated combined data saved to {existing_combined_file} with {len(combined_data)} total rows.")
