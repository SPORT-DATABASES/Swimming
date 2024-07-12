
import os
import aiohttp
import asyncio
import json
import brotli
import gzip
import polars as pl
import pymysql
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import zipfile
from tqdm.asyncio import tqdm
from concurrent.futures import ThreadPoolExecutor
from fake_useragent import UserAgent
import nest_asyncio
import random
import time

# Apply nest_asyncio to allow nested event loops
nest_asyncio.apply()

# Load environment variables
load_dotenv()

# Database connection details
host = 'sportsdb-sports-database-for-web-scrapes.g.aivencloud.com'
port = 16439
user = 'avnadmin'
password = os.getenv('DB_PASSWORD')
database = 'defaultdb'
ca_cert_path = 'ca.pem'

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

# Read swimmer IDs from the CSV file
csv_file_path = 'all_swimmers.csv'
swimmers_df = pd.read_csv(csv_file_path)
swimmer_ids = swimmers_df["id"].tolist()

# Asynchronous function to fetch results for a single swimmer with retry logic
async def fetch_results(session, swimmer_id, retries=5, backoff_factor=1.0):
    url = f"https://api.worldaquatics.com/fina/athletes/{swimmer_id}/results"
    headers = generate_headers()
    for attempt in range(retries):
        try:
            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    try:
                        data = await response.json()
                        return {"id": swimmer_id, "results": data.get("Results", [])}
                    except json.JSONDecodeError:
                        decompressed_content = await decompress_content(response)
                        data = json.loads(decompressed_content)
                        return {"id": swimmer_id, "results": data.get("Results", [])}
                elif response.status == 429:
                    print("Rate limited. Pausing for 60 seconds and changing header...")
                    await asyncio.sleep(60)
                    headers = generate_headers()  # Change headers
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
                pause_duration = random.uniform(30, 60)
                print(f"Pausing for {pause_duration:.2f} seconds after {i} requests...")
                await asyncio.sleep(pause_duration)  # Random pause after every 5000 requests
            tasks.append(fetch_results(session, swimmer_id))
            if len(tasks) >= 5000 or i == len(swimmer_ids) - 1:
                results = await asyncio.gather(*tasks)
                all_results.extend(results)
                failed_ids.extend([result["id"] for result in results if result.get("status") == "failed"])
                tasks = []
                # Add random pauses between batches
                pause_duration = random.uniform(1, 5)
                print(f"Pausing for {pause_duration:.2f} seconds between batches...")
                await asyncio.sleep(pause_duration)
        return all_results, failed_ids

# Main function to run the asynchronous fetching
async def main():
    all_results, failed_ids = await fetch_all_results(swimmer_ids)
    
    # Retry failed requests
    if failed_ids:
        print(f"Retrying {len(failed_ids)} failed requests...")
        retry_results, retry_failed_ids = await fetch_all_results(failed_ids)
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
    results_df = pd.DataFrame(flattened_results)

    # Save the DataFrame to a zipped CSV file for backup
    compression_options = dict(method='zip', archive_name='swimmers_results.csv')
    results_df.to_csv('swimmers_results.zip', index=False, compression=compression_options)
    print("CSV file compressed into ZIP successfully.")

    # Create SQLAlchemy engine
    engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}', connect_args={'ssl': {'ca': ca_cert_path}})

    # Function to create table and insert data in batches
    def create_and_insert_table(df, table_name, create_table_query, batch_size=50000):
        with engine.connect() as connection:
            print(f"Creating table {table_name}...")
            # Execute the CREATE TABLE statement
            connection.execute(text(create_table_query))
            
            # Truncate the table to remove all existing data
            connection.execute(text(f'TRUNCATE TABLE {table_name}'))
            print(f"Table {table_name} created and truncated.")
        
        # Function to insert a single batch of data
        def insert_batch(start, end):
            df.iloc[start:end].to_sql(table_name, con=engine, if_exists='append', index=False)
            print(f'Inserted rows {start} to {end} into {table_name}')

        # Insert data in batches with progress bar
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = []
            for start in tqdm(range(0, len(df), batch_size), desc=f'Inserting into {table_name}', unit='batch'):
                end = start + batch_size
                futures.append(executor.submit(insert_batch, start, end))
            
            # Wait for all futures to complete
            for future in tqdm(futures, desc='Waiting for batch insertions to complete'):
                future.result()

        print(f'Data inserted successfully for {table_name}.')

    # Define the CREATE TABLE statements
    create_table_all_swimmer = '''
    CREATE TABLE IF NOT EXISTS all_swimmer (
        `id` INT AUTO_INCREMENT PRIMARY KEY,
        `providerId` VARCHAR(255),
        `firstName` VARCHAR(255),
        `fullName` VARCHAR(255),
        `nationality` VARCHAR(255),
        `gender` VARCHAR(50),
        `disciplines` TEXT,
        `metadata` TEXT,
        `lastName` VARCHAR(255),
        `dateOfBirth` DATE,
        `height` FLOAT,
        `last_updated` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    )
    '''

    create_table_all_swim_results = '''
    CREATE TABLE IF NOT EXISTS all_swim_results (
        `id` INT AUTO_INCREMENT PRIMARY KEY,
        `swimmer_id` VARCHAR(255),
        `Rank` FLOAT,
        `MedalTag` VARCHAR(255),
        `SportCode` VARCHAR(255),
        `DisciplineName` VARCHAR(255),
        `PhaseName` VARCHAR(255),
        `RecordType` VARCHAR(255),
        `NAT` VARCHAR(255),
        `CompetitionName` VARCHAR(255),
        `CompetitionType` VARCHAR(255),
        `CompetitionCountry` VARCHAR(255),
        `CompetitionCity` VARCHAR(255),
        `Date` DATE,
        `Time` VARCHAR(255),
        `Tags` VARCHAR(255),
        `AthleteResultAge` FLOAT,
        `Points` FLOAT,
        `UtcDateTime` VARCHAR(255),
        `ClubName` VARCHAR(255),
        `Score` VARCHAR(255),
        `MatchName` VARCHAR(255),
        `TeamHome` VARCHAR(255),
        `TeamAway` VARCHAR(255),
        `TeamHomeCode` VARCHAR(255),
        `TeamAwayCode` VARCHAR(255),
        `FinalScoreHome` VARCHAR(255),
        `FinalScoreAway` VARCHAR(255),
        `last_updated` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    )
    '''

    # Create and insert data into the tables
    create_and_insert_table(swimmers_df, 'all_swimmer', create_table_all_swimmer)
    create_and_insert_table(results_df, 'all_swim_results', create_table_all_swim_results)

    print('Data inserted successfully for all tables.')

if __name__ == "__main__":
    asyncio.run(main())
