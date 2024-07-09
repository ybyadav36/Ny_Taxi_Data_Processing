import os
import requests
from tqdm import tqdm
from bs4 import BeautifulSoup
import pandas as pd
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
import logging
import queue

# Configure logging
logging.basicConfig(filename='download.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# Function to scrape URLs 
def get_taxi_data_urls(page_url, year):
    response = requests.get(page_url)
    soup = BeautifulSoup(response.content, 'html.parser')
    links = soup.find_all('a', href=True)
    urls = [link['href'] for link in links if f"yellow_tripdata_{year}" in link['href']]
    return urls

# Function to download and convert with retries and progress bar
def download_and_convert_to_csv(url, dest_folder, retries=3, timeout=60): # Increased timeout to 60 seconds
    base_filename = url.split('/')[-1].replace(".parquet", "")
    data_name_folder = os.path.join(dest_folder, base_filename)
    os.makedirs(data_name_folder, exist_ok=True)
    csv_file = os.path.join(data_name_folder, f"{base_filename}.csv")

    retry_strategy = Retry(
        total=retries,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    http = requests.Session()
    http.mount("https://", adapter)

    for attempt in range(retries):
        try:
            logging.info(f"Starting download attempt {attempt + 1}/{retries} for {base_filename}")  # Log start
            with http.get(url, stream=True, timeout=timeout) as r: # Added timeout
                r.raise_for_status()

                # Create in-memory buffer and read parquet (chunk size adjusted)
                total_size = int(r.headers.get('content-length', 0))
                block_size = 1024 * 1024  # 1 MB
                buffer = BytesIO()
                with tqdm(total=total_size, unit='iB', unit_scale=True, desc=f"Downloading & converting {base_filename}") as t:
                    for data in r.iter_content(block_size):
                        t.update(len(data))
                        buffer.write(data)
                    buffer.seek(0)
                    df = pd.read_parquet(buffer) 
                    df.to_csv(csv_file, index=False)

            logging.info(f"Downloaded and converted {base_filename} to CSV")
            return csv_file

        except requests.RequestException as e:
            logging.error(f"Error downloading {base_filename}: {e}")  # Log error
            if attempt < retries - 1:
                logging.info(f"Retrying... (Attempt {attempt + 1}/{retries})")
            else:
                logging.error(f"Failed to download {base_filename} after {retries} attempts.")
    return None


# Main function
def main():
    page_url = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
    year = "2019"
    dest_folder = f"data/{year}" 
    num_expected_files = 12

    print("Extracting URLs...")
    urls = get_taxi_data_urls(page_url, year)

    print(f"Found {len(urls)} files to download.")

    # Create a queue to hold the download tasks
    task_queue = queue.Queue()
    for url in urls:
        task_queue.put(url)  

    # Create ThreadPoolExecutor and submit tasks as workers become available
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = []
        while not task_queue.empty():
            url = task_queue.get()
            future = executor.submit(download_and_convert_to_csv, url, dest_folder)
            futures.append(future)

        # Wait for all tasks to complete and track results
        num_downloaded_files = 0
        for future in as_completed(futures):  
            result = future.result()
            if result is not None:  
                num_downloaded_files += 1

    # Check if all files were downloaded
    if num_downloaded_files == num_expected_files:
        print("Download completed for all 12 files.")
    else:
        print(f"Warning: Downloaded {num_downloaded_files} files, but expected {num_expected_files}.")


if __name__ == "__main__":
    main()
   