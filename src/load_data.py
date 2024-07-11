import sqlite3
import pandas as pd
import os
from tqdm import tqdm
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get database file path from environment variables
db_file = os.getenv('DB_FILE')

def connect_to_db(db_file):
    conn = sqlite3.connect(db_file)
    return conn

# Connect to the database
conn = connect_to_db(db_file)
cursor = conn.cursor()

# Create the trips table with progress bar
with tqdm(total=1, desc='Creating trips table') as pbar:
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS trips (
        date DATE NOT NULL,
        pickup_hour INTEGER NOT NULL,
        category TEXT NOT NULL,
        total_trips INTEGER NOT NULL,
        average_fare REAL NOT NULL,
        passenger_count INTEGER NOT NULL,
        trip_distance REAL NOT NULL,
        fare_amount REAL NOT NULL,
        trip_duration REAL NOT NULL,
        average_speed REAL NOT NULL,
        PRIMARY KEY (date, pickup_hour, category)
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
            df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')  # Ensure date is in correct string format
            with tqdm(total=len(df), desc=f'Loading {category} data for month {month:02d}') as pbar:
                for index, row in df.iterrows():
                    cursor.execute('''
                    REPLACE INTO trips (date, pickup_hour, category, total_trips, average_fare, passenger_count, trip_distance, fare_amount, trip_duration, average_speed)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        row['date'], 
                        int(row['pickup_hour']),
                        row['category'], 
                        int(row['total_trips']), 
                        float(row['average_fare']), 
                        int(row['passenger_count']), 
                        float(row['trip_distance']), 
                        float(row['fare_amount']), 
                        float(row['trip_duration']), 
                        float(row['average_speed'])
                    ))
                    pbar.update(1)
                conn.commit()

# Close the connection
conn.close()





