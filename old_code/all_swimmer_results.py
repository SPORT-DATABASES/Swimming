import requests
import json
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import brotli
import gzip

# Base URL for fetching results
base_url = "https://api.worldaquatics.com/fina/athletes/{}/results"

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

# Read swimmer IDs from the CSV file
swimmers_df = pd.read_csv("all_swimmers.csv")
swimmer_ids = swimmers_df["id"].tolist()

# Function to fetch results for a single swimmer
def fetch_results(swimmer_id):
    url = base_url.format(swimmer_id)
    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code == 200:
            try:
                # First try to parse the content as JSON directly
                data = response.json()
                return {"id": swimmer_id, "results": data.get("Results", [])}
            except json.JSONDecodeError:
                # If direct parsing fails, attempt decompression based on encoding
                decompressed_content = decompress_content(response)
                data = json.loads(decompressed_content)
                return {"id": swimmer_id, "results": data.get("Results", [])}
        else:
            print(f"Failed to retrieve results for swimmer ID {swimmer_id} with status code {response.status_code}.")
            return {"id": swimmer_id, "results": []}
    except requests.RequestException as e:
        print(f"Request failed for swimmer ID {swimmer_id}. Error: {e}")
        return {"id": swimmer_id, "results": []}

# Function to decompress content based on encoding
def decompress_content(response):
    encoding = response.headers.get('Content-Encoding')
    if encoding == 'br':
        return brotli.decompress(response.content)
    elif encoding == 'gzip':
        return gzip.decompress(response.content)
    elif encoding == 'deflate':
        return response.content.decode('deflate')
    else:
        return response.content

# List to store all results
all_results = []

# Increase the number of workers to speed up the scraping process
num_workers = 20

# Use ThreadPoolExecutor to parallelize requests
with ThreadPoolExecutor(max_workers=num_workers) as executor:
    # Submit tasks for each swimmer ID
    futures = [executor.submit(fetch_results, swimmer_id) for swimmer_id in swimmer_ids]
    
    # Use tqdm to show progress bar
    for future in tqdm(as_completed(futures), total=len(swimmer_ids)):
        result = future.result()
        all_results.append(result)

# Flatten the results for better DataFrame representation
flattened_results = []
for swimmer_result in all_results:
    swimmer_id = swimmer_result["id"]
    for result in swimmer_result["results"]:
        flattened_result = {"swimmer_id": swimmer_id}
        flattened_result.update(result)
        flattened_results.append(flattened_result)

# Convert the list of results to a DataFrame
df = pd.DataFrame(flattened_results)

# Save the DataFrame to a CSV file
df.to_csv("swimmers_results.csv", index=False)

print("Data successfully saved to swimmers_results.csv")
