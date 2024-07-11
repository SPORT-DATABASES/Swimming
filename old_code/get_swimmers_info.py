import aiohttp
import asyncio
import json
import brotli
import gzip
import pandas as pd
from tqdm.asyncio import tqdm
from fake_useragent import UserAgent
import nest_asyncio

# Apply nest_asyncio to allow nested event loops
nest_asyncio.apply()

# Base URL for athletes endpoint
base_url = "https://api.worldaquatics.com/fina/athletes"

# Function to generate headers with a fake user agent
def generate_headers():
    ua = UserAgent()
    return {
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate, br, zstd, br",
        "Accept-Language": "en-US,en;q=0.9",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Origin": "https://www.worldaquatics.com",
        "Referer": "https://www.worldaquatics.com/",
        "Sec-Ch-Ua": f'"Not/A)Brand";v="8", "Chromium";v="{ua.chrome.split("/")[1].split(".")[0]}", "Google Chrome";v="{ua.chrome.split("/")[1].split(".")[0]}"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": "\"Windows\"",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-site",
        "User-Agent": ua.random
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
async def fetch_page(session, page):
    retries = 3
    for attempt in range(retries):
        params["page"] = page
        async with session.get(base_url, headers=generate_headers(), params=params) as response:
            if response.status == 200:
                try:
                    decompressed_content = brotli.decompress(await response.read())
                    data = json.loads(decompressed_content)
                except brotli.error:
                    try:
                        data = await response.json()
                    except json.JSONDecodeError as e:
                        print(f"Failed to decode JSON response on page {page}. Error: {e}")
                        print("Raw response content:")
                        print(await response.text())
                        return []
                return data.get("content", [])
            elif response.status == 504:
                print(f"Gateway Timeout on page {page}. Retrying...")
                await asyncio.sleep(2 ** attempt)  # Exponential backoff
            else:
                print(f"Failed to retrieve page {page} with status code {response.status}. Response content:")
                print(await response.text())
                return []
    print(f"Failed to retrieve page {page} after {retries} attempts.")
    return None

# Initial request to get the number of pages
async def get_total_pages():
    async with aiohttp.ClientSession() as session:
        async with session.get(base_url, headers=generate_headers(), params=params) as response:
            if response.status == 200:
                try:
                    decompressed_content = brotli.decompress(await response.read())
                    data = json.loads(decompressed_content)
                except brotli.error:
                    try:
                        data = await response.json()
                    except json.JSONDecodeError as e:
                        print(f"Failed to decode JSON response on initial request. Error: {e}")
                        print("Raw response content:")
                        print(await response.text())
                        data = {}
                num_pages = data.get("pageInfo", {}).get("numPages", 0)
                print(f"Total number of pages: {num_pages}")
                return num_pages
            else:
                print(f"Failed to retrieve initial page with status code {response.status}. Response content:")
                print(await response.text())
                return 0

# Asynchronous function to fetch all athletes
async def fetch_all_athletes(num_pages):
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_page(session, page) for page in range(num_pages)]
        all_athletes = []
        failed_pages = []
        for future in tqdm(asyncio.as_completed(tasks), total=len(tasks)):
            athletes = await future
            if athletes is not None:
                all_athletes.extend(athletes)
            else:
                failed_pages.append(tasks.index(future))
        return all_athletes, failed_pages

# Function to retry failed pages
async def retry_failed_pages(failed_pages):
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_page(session, page) for page in failed_pages]
        all_athletes = []
        for future in tqdm(asyncio.as_completed(tasks), total=len(tasks)):
            athletes = await future
            if athletes is not None:
                all_athletes.extend(athletes)
        return all_athletes

# Main function to run the asynchronous fetching
async def main():
    num_pages = await get_total_pages()
    if num_pages > 0:
        all_athletes, failed_pages = await fetch_all_athletes(num_pages)
        
        if failed_pages:
            print(f"Retrying {len(failed_pages)} failed pages...")
            retry_athletes = await retry_failed_pages(failed_pages)
            all_athletes.extend(retry_athletes)

        # Save the data to a JSON file
        with open("all_swimmers.json", "w") as f:
            json.dump(all_athletes, f, indent=4)

        print(f"Total athletes retrieved: {len(all_athletes)}")

        # Convert JSON data to DataFrame and save to CSV
        all_swimmers = pd.DataFrame(all_athletes)
        all_swimmers.to_csv("all_swimmers.csv", index=False)
        print("Data successfully saved to all_swimmers.csv")

# Run the main function
if __name__ == "__main__":
    asyncio.run(main())






