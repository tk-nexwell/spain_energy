import os

# Folder containing .1 files
input_folder = "DayAheadMarketFiles"

# Limit the number of instances to find
max_instances = 5
found_instances = 0

# Process files to find unexpected rows
for file_name in os.listdir(input_folder):
    if file_name.endswith(".1"):
        file_path = os.path.join(input_folder, file_name)
        
        # Skip the first line (title) and last line (asterisk)
        with open(file_path, 'r') as f:
            lines = f.readlines()[1:-1]

        # Check for rows with unexpected column counts
        for line in lines:
            # Remove trailing semicolons and split
            columns = [value.strip() for value in line.strip(";").split(";")]
            if len(columns) != 6:  # Log rows with unexpected columns
                print(f"File: {file_name} | Row: {line.strip()}")
                found_instances += 1
                if found_instances >= max_instances:
                    break  # Stop after finding the limit
        if found_instances >= max_instances:
            break  # Stop processing further files

if found_instances == 0:
    print("No unexpected rows found. All rows have 6 columns.")
