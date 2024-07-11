import os
import polars as pl
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import zipfile
from tqdm import tqdm

load_dotenv()

# Database connection details
host = 'sportsdb-sports-database-for-web-scrapes.g.aivencloud.com'
port = 16439
user = 'avnadmin'
password = os.getenv('DB_PASSWORD')
database = 'defaultdb'
ca_cert_path = 'ca.pem'

# Load the CSV file using polars
csv_file_path = 'all_swimmers.csv'
all_swimmers_df = pl.read_csv(csv_file_path)

# Extract the zip file to the current working directory
zip_file_path = 'swimmers_results.zip'

with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
    zip_ref.extractall()  # Extracts to the current working directory

# Assuming the extracted files are CSV files
extracted_files = [file for file in os.listdir() if file.endswith('.csv')]

# Define the data types for the problematic columns
dtype_dict = {
    'swimmer_id': str,
    'Rank': float,
    'MedalTag': str,
    'SportCode': str,
    'DisciplineName': str,
    'PhaseName': str,
    'RecordType': str,
    'NAT': str,
    'CompetitionName': str,
    'CompetitionType': str,
    'CompetitionCountry': str,
    'CompetitionCity': str,
    'Date': str,
    'Time': str,
    'Tags': str,
    'AthleteResultAge': float,
    'Points': float,
    'UtcDateTime': str,
    'ClubName': str,
    'Score': str,
    'MatchName': str,
    'TeamHome': str,
    'TeamAway': str,
    'TeamHomeCode': str,
    'TeamAwayCode': str,
    'FinalScoreHome': str,
    'FinalScoreAway': str
}

# Load the extracted CSV files into DataFrames with specified dtypes using polars
all_swim_results_dfs = [pl.read_csv(file, dtypes=dtype_dict) for file in extracted_files]

# Concatenate all the dataframes into a single dataframe
all_swim_results_df = pl.concat(all_swim_results_dfs)

# Convert polars DataFrames to pandas DataFrames for SQLAlchemy compatibility
all_swimmers_df = all_swimmers_df.to_pandas()
all_swim_results_df = all_swim_results_df.to_pandas()

# Create SQLAlchemy engine
engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}', connect_args={'ssl': {'ca': ca_cert_path}})

# Function to create table and insert data in batches
def create_and_insert_table(df, table_name, create_table_query, batch_size=10000):
    with engine.connect() as connection:
        print(f"Creating table {table_name}...")
        # Execute the CREATE TABLE statement
        connection.execute(text(create_table_query))
        
        # Truncate the table to remove all existing data
        connection.execute(text(f'TRUNCATE TABLE {table_name}'))
        print(f"Table {table_name} created and truncated.")
        
    # Insert data in batches with progress bar
    for start in tqdm(range(0, len(df), batch_size), desc=f'Inserting into {table_name}', unit='batch'):
        end = start + batch_size
        df.iloc[start:end].to_sql(table_name, con=engine, if_exists='append', index=False)
        print(f'Inserted rows {start} to {end} into {table_name}')
        
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
create_and_insert_table(all_swimmers_df, 'all_swimmer', create_table_all_swimmer)
create_and_insert_table(all_swim_results_df, 'all_swim_results', create_table_all_swim_results)

print('Data inserted successfully for all tables.')

