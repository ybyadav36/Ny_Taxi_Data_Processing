import sqlite3
import pandas as pd
import os
from tqdm import tqdm
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get database credentials from environment variables
db_credentials = {
    'username': os.getenv('DB_USERNAME'),
    'password': os.getenv('DB_PASSWORD'),
    'db_file': os.getenv('DB_FILE')
}

def connect_to_db(credentials):
    # Simulate credential check (SQLite doesn't use username/password)
    if credentials['username'] == os.getenv('DB_USERNAME') and credentials['password'] == os.getenv('DB_PASSWORD'):
        conn = sqlite3.connect(credentials['db_file'])
        return conn
    else:
        raise ValueError("Invalid credentials")

# Connect to the database
conn = connect_to_db(db_credentials)
cursor = conn.cursor()

# Create the trips table with progress bar
with tqdm(total=1, desc='Creating trips table') as pbar:
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS trips (
        date DATE NOT NULL,
        category TEXT NOT NULL,
        total_trips INTEGER NOT NULL,
        average_fare REAL NOT NULL,
        passenger_count INTEGER NOT NULL,
        trip_distance REAL NOT NULL,
        fare_amount REAL NOT NULL,
        trip_duration REAL NOT NULL,
        average_speed REAL NOT NULL,
        PRIMARY KEY (date, category)
    )
    ''')
    conn.commit()
    pbar.update(1)

# Load CSV files and insert data into the database with progress bar
processed_folder = 'processed_data'
categories = ['yellow', 'green']

for category in categories:
    for month in range(1, 13):
        file_path = os.path.join(processed_folder, category, f'{category}_processed_data_2019_{month:02d}.csv')
        if os.path.exists(file_path):
            df = pd.read_csv(file_path)
            df['category'] = category
            with tqdm(total=len(df), desc=f'Loading {category} data for month {month:02d}') as pbar:
                for index, row in df.iterrows():
                    cursor.execute('''
                    REPLACE INTO trips (date, category, total_trips, average_fare, passenger_count, trip_distance, fare_amount, trip_duration, average_speed)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (row['date'], row['category'], row['total_trips'], row['average_fare'], row['passenger_count'], row['trip_distance'], row['fare_amount'], row['trip_duration'], row['average_speed']))
                    pbar.update(1)
                conn.commit()

# Close the connection
conn.close()

