import os
import requests
from bs4 import BeautifulSoup

# URL of the directory containing the files
url = "https://www.omie.es/en/file-access-list?parents=/Day-ahead%20Market/1.%20Prices&dir=%20Day-ahead%20market%20hourly%20prices%20in%20Spain&realdir=marginalpdbc"

# Headers to mimic a browser visit
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

# Folder to save files
output_folder = "DayAheadMarketFiles"
os.makedirs(output_folder, exist_ok=True)

# Fetch the webpage content
response = requests.get(url, headers=headers)
if response.status_code == 200:
    soup = BeautifulSoup(response.content, 'html.parser')
    # Find all links to .1 files
    file_links = soup.find_all('a', href=True)
    
    for link in file_links:
        file_url = link['href']
        if file_url.endswith('.1'):  # Only download .1 files
            # Full download URL
            full_url = f"https://www.omie.es{file_url}"
            
            # Extract filename and sanitize it
            raw_filename = os.path.basename(file_url)
            sanitized_filename = raw_filename.replace('?', '_').replace('=', '_')
            file_path = os.path.join(output_folder, sanitized_filename)

            # Download the file
            print(f"Downloading: {sanitized_filename}")
            file_response = requests.get(full_url)
            if file_response.status_code == 200:
                with open(file_path, 'wb') as f:
                    f.write(file_response.content)
                print(f"Saved: {file_path}")
            else:
                print(f"Failed to download: {sanitized_filename}")
else:
    print("Failed to fetch the webpage.")

print(f"All files downloaded to {os.path.abspath(output_folder)}")
