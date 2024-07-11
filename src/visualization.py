import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from tqdm import tqdm
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Get database file path from environment variables
db_file = os.getenv('DB_FILE')

def connect_to_db(db_file):
    conn = sqlite3.connect(db_file)
    return conn

def show_query_results(df, query_name):
    print(f"\nResults for {query_name} query:")
    print(df.head())

# Connect to the database
conn = connect_to_db(db_file)

# Define and execute queries with progress bars
queries = {
    'peak_hours': '''
    SELECT pickup_hour AS hour, 
           category, 
           SUM(total_trips) AS total_trips
    FROM trips
    GROUP BY hour, category
    ''',  
    'passenger_fare': '''
    SELECT category, passenger_count, AVG(fare_amount) AS average_fare
    FROM trips
    WHERE passenger_count > 0  
    GROUP BY category, passenger_count
    ORDER BY category, passenger_count
    ''',
    'usage_trends': '''
    SELECT strftime('%Y-%m', date) AS month, category, SUM(total_trips) AS total_trips
    FROM trips
    GROUP BY month, category
    ORDER BY month, category
    '''
}

for query_name, query in queries.items():
    with tqdm(total=1, desc=f'Executing {query_name} query') as pbar:
        df = pd.read_sql_query(query, conn)
        pbar.update(1)

        # Shows the query results in the terminal
        show_query_results(df, query_name)

        # Handles missing hours for 'peak_hours'
        if query_name == 'peak_hours':
            all_hours = pd.DataFrame({'hour': range(24)})
            df = pd.merge(all_hours, df, on='hour', how='left').fillna(0)
            df['hour'] = df['hour'].astype(int)
            df.sort_values(by='hour', inplace=True)

        # Bin passenger counts for 'passenger_fare'
        elif query_name == 'passenger_fare':
            bins = [0, 1, 2, 3, 4, 5, 6, 10, 20, 50, 100, 500, 1000]
            labels = ['1', '2', '3', '4', '5', '6', '7-10', '11-20', '21-50', '51-100', '101-500', '501-1000']
            df['passenger_count_bin'] = pd.cut(df['passenger_count'], bins=bins, labels=labels)
            df = df.groupby(['category', 'passenger_count_bin']).agg({'average_fare': 'mean'}).reset_index()

        # Handle month order for 'usage_trends'
        elif query_name == 'usage_trends':
            df['month'] = pd.Categorical(df['month'], categories=df['month'].unique(), ordered=True)

        # Plotting
        plt.figure(figsize=(12, 6))
        if query_name == 'peak_hours':
            sns.barplot(x='hour', y='total_trips', hue='category', data=df, palette='viridis')
            plt.title('Peak Hours for Taxi Usage')
            plt.xlabel('Hour of Day')
            plt.ylabel('Total Trips')
            plt.xticks(ticks=range(24), labels=range(24), rotation=45)
            plt.grid(axis='y')
        elif query_name == 'passenger_fare':
            sns.barplot(x='passenger_count_bin', y='average_fare', hue='category', data=df, palette='viridis')
            plt.title('Effect of Passenger Count on Trip Fare')
            plt.xlabel('Passenger Count')
            plt.ylabel('Average Fare')
            plt.xticks(rotation=45, ha="right")
            plt.grid(axis='y')
        elif query_name == 'usage_trends':
            sns.lineplot(x='month', y='total_trips', hue='category', data=df, marker='o')
            plt.title('Trends in Usage Over the Year')
            plt.xlabel('Month')
            plt.ylabel('Total Trips')
            plt.grid(axis='y')

        plt.tight_layout()
        plt.show()

# Close the connection
conn.close()















