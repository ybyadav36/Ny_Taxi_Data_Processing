#Project Overview
This project establishes a robust data engineering pipeline to analyze New York City's Yellow and Green Taxi Trip data for the year 2019. The pipeline encompasses:

1-Automated downloading of monthly trip datasets in Parquet format.
2-Conversion of Parquet data to CSV for easier processing.
3-Data cleaning and transformation to derive insights such as trip duration, average speed, and peak hours.
4-Aggregation of data to analyze daily trends and the impact of passenger count on fares.
5-Loading the processed data into an SQLite database for efficient querying and analysis.
6-Generation of insightful visualizations to understand peak usage hours, fare trends, and passenger count effects.

##Note-  data download and data processing script will take time to download data and covert them to csv as source data is in perquet format and process data script will take time to process data as it has large valume (i can make processing fater by parallelizing processing task deviding them between 3-5 workers but due to heavy load it's not running on my syetm which has only 8gb of RAM)

##Environment Setup - 
Python: Ensure you have Python 3.x installed. You can download it from https://www.python.org/.
Virtual Environment (Recommended): Create and activate a Python virtual environment to isolate project dependencies:

python -m venv env
source env/bin/activate  # On macOS/Linux
.\env\Scripts\activate  # On Windows

Libraries: Install the required Python packages:

pip install -r requirements.txt

SQLite: Make sure you have SQLite installed.

##Running the Project
Download Data:
python src/download_data.py
This script will fetch the monthly trip data from the NYC TLC website, convert it from Parquet to CSV, and organize it into folders by category and month.
Process Data:
python src/process_data.py
This script will clean the raw CSV files, create new features, filter out invalid data, aggregate the data by date and hour, and save the processed data into CSV files in the processed_data folder.
Load Data to SQLite Database:
python src/load_data.py

Analyze and Visualize Data:
python src/visualize.py
This script will connect to the database, execute queries to retrieve insights, and generate visualizations using Matplotlib and Seaborn.

##Data Analysis
The visualize.py script executes SQL queries to answer the following questions:

Peak Hours for Taxi Usage:  Identifies the hours with the highest number of trips, broken down by taxi type (yellow/green). The results are visualized in a bar chart.

Passenger Count vs. Trip Fare: Examines the relationship between the number of passengers and the average trip fare. Visualized with a bar chart (or scatterplot if needed).

Trends in Usage Over the Year: Tracks the total number of trips per month for both taxi types, showcasing the changes in usage patterns throughout the year. A line chart illustrates this trend.

