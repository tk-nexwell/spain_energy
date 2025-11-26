import os

# Assuming the script is in the SPAIN folder
CURRENT_FOLDER = os.path.dirname(os.path.abspath(__file__))
BCC_FILE = os.path.join(CURRENT_FOLDER, "bcc_list.txt")

# Debugging steps
try:
    print(f"Looking for Bcc file at: {os.path.abspath(BCC_FILE)}")

    # Check if the file exists
    if not os.path.exists(BCC_FILE):
        print("Error: Bcc file does not exist at the specified path.")
        raise FileNotFoundError(f"Bcc file not found: {BCC_FILE}")
    else:
        print("Bcc file found successfully.")

    # Check if the file is readable
    with open(BCC_FILE, "r") as file:
        lines = file.readlines()

    # Validate contents
    if not lines:
        print("Error: Bcc file is empty.")
    else:
        print("Bcc file contents:")
        for line in lines:
            email = line.strip()
            if email:
                print(f"- {email}")
            else:
                print("Warning: Empty line found in Bcc file.")

except Exception as e:
    print(f"An error occurred: {e}")
