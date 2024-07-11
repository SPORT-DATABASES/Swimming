import requests
import json
import brotli
import gzip
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import pandas as pd

# Base URL for athletes endpoint
base_url = "https://api.worldaquatics.com/fina/athletes"

# Headers as provided
headers = {
    "Accept": "*/*",
    "Accept-Encoding": "gzip, deflate, br, zstd, br",
    "Accept-Language": "en-US,en;q=0.9",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "Origin": "https://www.worldaquatics.com",
    "Referer": "https://www.worldaquatics.com/",
    "Sec-Ch-Ua": "\"Not/A)Brand\";v=\"8\", \"Chromium\";v=\"126\", \"Google Chrome\";v=\"126\"",
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": "\"Windows\"",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-site",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
}

# Parameters for the initial request to get the number of pages
params = {
    "gender": "",
    "discipline": "SW",
    "nationality": "",
    "name": "",
    "pageSize": 50,
    "page": 0
}

# Function to fetch data for a specific page
def fetch_page(page):
    params["page"] = page
    response = requests.get(base_url, headers=headers, params=params)
    if response.status_code == 200:
        try:
            decompressed_content = brotli.decompress(response.content)
            data = json.loads(decompressed_content)
        except brotli.error:
            try:
                data = json.loads(response.content)
            except json.JSONDecodeError as e:
                print(f"Failed to decode JSON response on page {page}. Error: {e}")
                print("Raw response content:")
                print(response.content)
                return []
        return data.get("content", [])
    else:
        print(f"Failed to retrieve page {page} with status code {response.status_code}. Response content:")
        print(response.content)
        return []

# Initial request to get the number of pages
response = requests.get(base_url, headers=headers, params=params)
if response.status_code == 200:
    try:
        decompressed_content = brotli.decompress(response.content)
        data = json.loads(decompressed_content)
    except brotli.error:
        try:
            data = json.loads(response.content)
        except json.JSONDecodeError as e:
            print(f"Failed to decode JSON response on initial request. Error: {e}")
            print("Raw response content:")
            print(response.content)
            data = {}
    num_pages = data.get("pageInfo", {}).get("numPages", 0)
    print(f"Total number of pages: {num_pages}")
else:
    print(f"Failed to retrieve initial page with status code {response.status_code}. Response content:")
    print(response.content)
    num_pages = 0

# List to store all athletes
all_athletes = []

# Use ThreadPoolExecutor to parallelize requests
with ThreadPoolExecutor(max_workers=10) as executor:
    # Submit tasks for each page
    futures = [executor.submit(fetch_page, page) for page in range(num_pages)]
    
    # Use tqdm to show progress bar
    for future in tqdm(as_completed(futures), total=num_pages):
        athletes = future.result()
        all_athletes.extend(athletes)

# Save the data to a JSON file
with open("all_swimmers.json", "w") as f:
    json.dump(all_athletes, f, indent=4)

print(f"Total athletes retrieved: {len(all_athletes)}")

all_swimmers = pd.DataFrame(all_athletes)

# Save the DataFrame to a CSV file
all_swimmers.to_csv("all_swimmers.csv", index=False)

print("Data successfully saved to all_swimmers.csv")
