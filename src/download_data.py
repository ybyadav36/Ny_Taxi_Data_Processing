import os
import requests
import logging
from tqdm import tqdm
from bs4 import BeautifulSoup
import pandas as pd
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

# Configure logging
logging.basicConfig(filename='download.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Function to scrape URLs for different categories
def get_taxi_data_urls(page_url, year, category):
    response = requests.get(page_url)
    soup = BeautifulSoup(response.content, 'html.parser')
    links = soup.find_all('a', href=True)
    urls = [link['href'] for link in links if f"{category}_tripdata_{year}" in link['href']]
    return urls

# Function to download and convert with retries, timeout, and logging
def download_and_convert_to_csv(url, dest_folder, progress_bar, retries=3, timeout=60): # Increased timeout to 60 seconds
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

                # Create in-memory buffer and read parquet 
                total_size = int(r.headers.get('content-length', 0))
                block_size = 1024 * 1024  
                buffer = BytesIO()
                with tqdm(total=total_size, unit='iB', unit_scale=True, desc=f"Downloading {base_filename}", leave=False) as t:
                    for data in r.iter_content(block_size):
                        t.update(len(data))
                        buffer.write(data)
                    buffer.seek(0)
                    df = pd.read_parquet(buffer) 
                    df.to_csv(csv_file, index=False)

            logging.info(f"Downloaded and converted {base_filename} to CSV")
            tqdm.write(f"Downloaded and converted {base_filename} to CSV")
            progress_bar.update(1)  # To update the progress bar
            return csv_file

        except requests.RequestException as e:
            logging.error(f"Error downloading {base_filename}: {e}")  # Log error
            tqdm.write(f"Error downloading {base_filename}: {e}")
            if attempt < retries - 1:
                logging.info(f"Retrying... (Attempt {attempt + 1}/{retries})")
                tqdm.write(f"Retrying... (Attempt {attempt + 1}/{retries})")
            else:
                logging.error(f"Failed to download {base_filename} after {retries} attempts.")
                tqdm.write(f"Failed to download {base_filename} after {retries} attempts.")
    return None

# Main function
def main():
    page_url = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
    year = "2019"
    categories = ["yellow", "green"]  
    dest_folder = f"data/{year}"

    print("Extracting URLs...")
    all_urls = []
    category_folders = {}
    for category in categories:
        urls = get_taxi_data_urls(page_url, year, category)
        category_folder = os.path.join(dest_folder, category)
        category_folders[category] = category_folder
        all_urls.extend((url, category_folder) for url in urls)

    print(f"Found {len(all_urls)} files to download across all categories.")

    # Create a progress bar for tracking the download and conversion process
    progress_bar = tqdm(total=len(all_urls), desc="Total Progress", unit="file")

    # Create ThreadPoolExecutor and download concurrently
    max_workers = 5  
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(download_and_convert_to_csv, url, folder, progress_bar): url for url, folder in all_urls}
        num_downloaded_files = 0
        for future in as_completed(futures):
            result = future.result()
            if result is not None:  
                num_downloaded_files += 1

    # Checks if all files were downloaded
    if num_downloaded_files == len(all_urls):
        print(f"Download completed for all {len(all_urls)} files.")
    else:
        print(f"Warning: Downloaded {num_downloaded_files} files, but expected {len(all_urls)}.")

    progress_bar.close()  # Close the progress bar

if __name__ == "__main__":
    main()

   