import os
import pandas as pd
import logging
from datetime import datetime
from tqdm import tqdm

# Configure logging
logging.basicConfig(filename='process.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# file paths and column names
data_folder = 'data/2019'
output_folder = 'processed_data'
os.makedirs(output_folder, exist_ok=True)

yellow_columns = ['VendorID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'passenger_count', 'trip_distance', 'RatecodeID', 'store_and_fwd_flag', 'PULocationID', 'DOLocationID', 'payment_type', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'improvement_surcharge', 'total_amount', 'congestion_surcharge', 'airport_fee']
green_columns = ['VendorID', 'lpep_pickup_datetime', 'lpep_dropoff_datetime', 'store_and_fwd_flag', 'RatecodeID', 'PULocationID', 'DOLocationID', 'passenger_count', 'trip_distance', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'ehail_fee', 'improvement_surcharge', 'total_amount', 'payment_type', 'trip_type', 'congestion_surcharge']

# Function to process CSV files and save the processed data to CSV
def process_csv_files(category, columns):
    category_folder = os.path.join(data_folder, category)
    for month in range(1, 13):
        month_folder = f'{category}_tripdata_2019-{month:02d}'
        monthly_data = []
        month_folder_path = os.path.join(category_folder, month_folder)
        
        for subdir, dirs, files in os.walk(month_folder_path):
            for file in tqdm(files, desc=f'Processing {category} files for {month:02d}'):
                if file.endswith('.csv'):
                    file_path = os.path.join(subdir, file)
                    try:
                        df = pd.read_csv(file_path, dtype={columns[3]: 'str'}, low_memory=False)  # passenger_count as string initially

                        # Handle Inconsistent Datetime Formats
                        for col in columns[1:3]:  # pickup and dropoff columns (indices 1 and 2)
                            try:
                                df[col] = pd.to_datetime(df[col])
                            except ValueError:
                                logging.warning(f"Invalid datetime values found in column '{col}' of {file_path}. Dropping invalid rows.")
                                df = df.loc[pd.to_datetime(df[col], errors='coerce').notnull()]  # Drop rows with invalid datetime values
                                df[col] = pd.to_datetime(df[col])  # Convert valid values

                        # Filter out rows with unrealistic dates
                        start_date = datetime(2019, month, 1)
                        end_date = datetime(2019, month, 28 if month == 2 else (30 if month in [4, 6, 9, 11] else 31))
                        df = df[(df[columns[1]] >= start_date) & (df[columns[1]] <= end_date)]

                        # Convert passenger_count to float and handle conversion errors
                        df[columns[3]] = pd.to_numeric(df[columns[3]], errors='coerce').fillna(0).astype(int)

                        # Derive new columns
                        pickup_col = columns[1]
                        dropoff_col = columns[2]
                        df['trip_duration'] = (df[dropoff_col] - df[pickup_col]).dt.total_seconds() / 3600
                        df['pickup_hour'] = df[pickup_col].dt.hour
                        df['day_of_week'] = df[pickup_col].dt.dayofweek
                        df['month'] = df[pickup_col].dt.month
                        df['year'] = df[pickup_col].dt.year

                        # Calculate average speed and filter out extreme outliers
                        df['average_speed'] = df['trip_distance'] / df['trip_duration']
                        df['average_speed'] = df['average_speed'].replace([float('inf'), -float('inf')], 0)

                        # Filter for more realistic speeds 
                        speed_threshold_low = df['average_speed'].quantile(0.01)
                        speed_threshold_high = df['average_speed'].quantile(0.99)
                        df = df[(df['average_speed'] >= speed_threshold_low) & (df['average_speed'] <= speed_threshold_high)]

                        # Aggregate data by date and hour
                        df['date'] = df[pickup_col].dt.date
                        hourly_agg = df.groupby(['date', 'pickup_hour']).agg(
                            total_trips=pd.NamedAgg(column='VendorID', aggfunc='count'),
                            average_fare=pd.NamedAgg(column='fare_amount', aggfunc='mean'),
                            passenger_count=pd.NamedAgg(column='passenger_count', aggfunc='sum'),
                            trip_distance=pd.NamedAgg(column='trip_distance', aggfunc='sum'),
                            fare_amount=pd.NamedAgg(column='fare_amount', aggfunc='sum'),
                            trip_duration=pd.NamedAgg(column='trip_duration', aggfunc='sum'),
                            average_speed=pd.NamedAgg(column='average_speed', aggfunc='mean')
                        ).reset_index()

                        # Ensure passenger_count is integer
                        hourly_agg['passenger_count'] = hourly_agg['passenger_count'].astype(int)

                        # Round float columns to 2 decimal points
                        float_cols = ['trip_distance', 'average_fare', 'fare_amount', 'trip_duration', 'average_speed']
                        hourly_agg[float_cols] = hourly_agg[float_cols].round(2)

                        monthly_data.append(hourly_agg)

                    except (FileNotFoundError, pd.errors.EmptyDataError, pd.errors.ParserError) as e:
                        logging.error(f"Error processing {file_path} ({category}): {e}")

        if monthly_data:
            result_df = pd.concat(monthly_data)
            output_file = os.path.join(output_folder, category, f'{category}_processed_data_2019_{month:02d}.csv')
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            result_df.to_csv(output_file, index=False)
            print(f"Processed and saved: {output_file}")
        else:
            logging.info(f"No data found for {category} in month {month:02d}")

# Process yellow taxi data
process_csv_files('yellow', yellow_columns)

# Process green taxi data
process_csv_files('green', green_columns)



















