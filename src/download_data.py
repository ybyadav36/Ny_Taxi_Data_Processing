import os
import requests
from tqdm import tqdm
from bs4 import BeautifulSoup

# Function to scrape the URLs from the TLC data page
def get_taxi_data_urls(page_url, year):
    response = requests.get(page_url)
    soup = BeautifulSoup(response.content, 'html.parser')
    links = soup.find_all('a', href=True)
    urls = [link['href'] for link in links if f"yellow_tripdata_{year}" in link['href']]
    return urls

# Function to download a file with retries
def download_file(url, dest_folder, retries=3):
    if not os.path.exists(dest_folder):
        os.makedirs(dest_folder)
    local_filename = os.path.join(dest_folder, url.split('/')[-1])
    
    for attempt in range(retries):
        try:
            with requests.get(url, stream=True) as r:
                r.raise_for_status()
                with open(local_filename, 'wb') as f:
                    for chunk in tqdm(r.iter_content(chunk_size=8192)):
                        if chunk:
                            f.write(chunk)
            return local_filename
        except requests.RequestException as e:
            print(f"Error downloading {url}: {e}")
            if attempt < retries - 1:
                print(f"Retrying... (Attempt {attempt + 1}/{retries})")
            else:
                print(f"Failed to download {url} after {retries} attempts.")
    return None

# Main function to scrape URLs and download files
def main():
    page_url = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
    year = "2019"
    dest_folder = f"data/{year}"
    
    urls = get_taxi_data_urls(page_url, year)
    
    for url in urls:
        download_file(url, dest_folder)

if __name__ == "__main__":
    main()