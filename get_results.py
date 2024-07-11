import aiohttp
import asyncio
import pandas as pd
from tqdm.asyncio import tqdm
import json
import brotli
import gzip
import nest_asyncio
from fake_useragent import UserAgent

#ggggggggg

# Apply nest_asyncio to allow nested event loops
nest_asyncio.apply()

# Base URL for fetching results
base_url = "https://api.worldaquatics.com/fina/athletes/{}/results"

# Function to generate random headers
def generate_headers():
    ua = UserAgent()
    headers = {
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
    return headers

# Read swimmer IDs from the CSV file
swimmers_df = pd.read_csv("all_swimmers.csv")
swimmer_ids = swimmers_df["id"].tolist()

# Asynchronous function to fetch results for a single swimmer with retry logic
async def fetch_results(session, swimmer_id, retries=5, backoff_factor=1.0):
    url = base_url.format(swimmer_id)
    headers = generate_headers()
    for attempt in range(retries):
        try:
            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    try:
                        # First try to parse the content as JSON directly
                        data = await response.json()
                        return {"id": swimmer_id, "results": data.get("Results", [])}
                    except json.JSONDecodeError:
                        # If direct parsing fails, attempt decompression based on encoding
                        decompressed_content = await decompress_content(response)
                        data = json.loads(decompressed_content)
                        return {"id": swimmer_id, "results": data.get("Results", [])}
                elif response.status == 429:
                    retry_after = int(response.headers.get("Retry-After", backoff_factor))
                    print(f"Rate limited. Retrying after {retry_after} seconds...")
                    await asyncio.sleep(retry_after)
                else:
                    print(f"Failed to retrieve results for swimmer ID {swimmer_id} with status code {response.status}.")
                    return {"id": swimmer_id, "results": [], "status": "failed"}
        except aiohttp.ClientError as e:
            print(f"Request failed for swimmer ID {swimmer_id}. Error: {e}")
            await asyncio.sleep(backoff_factor * (2 ** attempt))
    return {"id": swimmer_id, "results": [], "status": "failed"}

# Function to decompress content based on encoding
async def decompress_content(response):
    encoding = response.headers.get('Content-Encoding')
    content = await response.read()
    if encoding == 'br':
        return brotli.decompress(content)
    elif encoding == 'gzip':
        return gzip.decompress(content)
    elif encoding == 'deflate':
        return content.decode('deflate')
    else:
        return content

# Asynchronous function to fetch all results
async def fetch_all_results(swimmer_ids):
    async with aiohttp.ClientSession() as session:
        tasks = []
        all_results = []
        failed_ids = []
        for i, swimmer_id in enumerate(tqdm(swimmer_ids)):
            if i > 0 and i % 5000 == 0:
                print(f"Pausing for 30 seconds after {i} requests...")
                await asyncio.sleep(30)  # Pause for 60 seconds after every 5000 requests
            tasks.append(fetch_results(session, swimmer_id))
            if len(tasks) >= 5000 or i == len(swimmer_ids) - 1:
                results = await asyncio.gather(*tasks)
                all_results.extend(results)
                failed_ids.extend([result["id"] for result in results if result.get("status") == "failed"])
                tasks = []
        return all_results, failed_ids

# Main function to run the asynchronous fetching
def main():
    loop = asyncio.get_event_loop()
    all_results, failed_ids = loop.run_until_complete(fetch_all_results(swimmer_ids))
    
    # Retry failed requests
    if failed_ids:
        print(f"Retrying {len(failed_ids)} failed requests...")
        retry_results, retry_failed_ids = loop.run_until_complete(fetch_all_results(failed_ids))
        all_results.extend(retry_results)
        failed_ids = retry_failed_ids

    # Flatten the results for better DataFrame representation
    flattened_results = []
    for swimmer_result in all_results:
        if swimmer_result["results"]:
            swimmer_id = swimmer_result["id"]
            for result in swimmer_result["results"]:
                flattened_result = {"swimmer_id": swimmer_id}
                flattened_result.update(result)
                flattened_results.append(flattened_result)

    # Convert the list of results to a DataFrame
    df = pd.DataFrame(flattened_results)

    # Save the DataFrame to a CSV file
    df.to_csv("swimmers_results_uncompressed.csv", index=False)
    print("Data successfully saved to swimmers_results_uncompressed.csv")

    # Compress the CSV file
    compression_options = dict(method='zip', archive_name='swimmers_results.csv')
    df.to_csv('swimmers_results.zip', index=False, compression=compression_options)
    print("CSV file compressed into ZIP successfully.")

if __name__ == "__main__":
    main()

