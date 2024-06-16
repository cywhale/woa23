import os
import logging
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from tqdm import tqdm

# Define paths and parameters
parameters = {
    # 't': 'temperature', #when download 1-degree, all time spans, disable temperature, salinity
    # 's': 'salinity',    #see https://www.ncei.noaa.gov/data/oceans/woa/WOA23/DOCUMENTATION/WOA23_Product_Documentation.pdf
    'o': 'oxygen',
    'O': 'o2sat',
    'A': 'AOU',
    'i': 'silicate',
    'p': 'phosphate',
    'n': 'nitrate'
}
time_periods = {
    '0': 'annual',
    '1': 'january',
    '2': 'february',
    '3': 'march',
    '4': 'april',
    '5': 'may',
    '6': 'june',
    '7': 'july',
    '8': 'august',
    '9': 'september',
    '10': 'october',
    '11': 'november',
    '12': 'december',
    '13': 'winter',
    '14': 'spring',
    '15': 'summer',
    '16': 'autumn'
}
grid_resolutions = {
    '01': '1.00',
    '04': '0.25'
}

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()
file_handler = logging.FileHandler('download_processing.log', mode='w')
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(file_handler)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(console_handler)

completed_downloads = set()

# Function to read completed downloads
def load_completed_downloads(res):
    global completed_downloads
    completed_data_file = f'data_downloaded_{res}.txt'
    if os.path.exists(completed_data_file):
        with open(completed_data_file, 'r') as f:
            for line in f:
                completed_downloads.add(line.strip())

# Function to write a completed download entry
def write_completed_download(param, period, grid_res, nc_file):
    completed_data_file = f'data_downloaded_{grid_res}.txt'
    with open(completed_data_file, 'a') as f:
        f.write(f'{param},{period},{grid_res},{nc_file}\n')

# Function to download data with retry, timeout, and progress logging
def download_data(url, save_path, retries=3, timeout=300, chunk_size=1024*1024):
    session = requests.Session()
    retry = Retry(total=retries, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)

    try:
        response = session.get(url, timeout=timeout, stream=True)
        response.raise_for_status()  # Raise HTTPError for bad responses

        total_size = int(response.headers.get('content-length', 0))
        downloaded_size = 0

        with open(save_path, 'wb') as f, tqdm(total=total_size, unit='B', unit_scale=True, desc=save_path, ncols=100) as pbar:
            for data in response.iter_content(chunk_size=chunk_size):
                f.write(data)
                downloaded_size += len(data)
                pbar.update(len(data))

    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to download {url}: {e}")
        raise

# Function to check if a dataset has been downloaded
def is_data_downloaded(param, period, grid_res):
    return f'{param},{period},{grid_res}' in completed_downloads

# Function to download WOA2023 datasets
# decav: 1955-2022, Average of seven decadal means from 1955 to 2022.
# all: All available years. Average of all available data. Refers to the 1965-2022 time span for dissolved oxygen (and related fields) and nutrients
def download_woa23_datasets(save_dir):
    grids = ["01"] # ["04", "01"]  # Download 0.25-degree data first
    # base_url = "https://www.ncei.noaa.gov/data/oceans/woa/WOA23/DATA" # for 0.25-degree
    base_url = "https://www.ncei.noaa.gov/thredds-ocean/fileServer/woa23/DATA" #for 1.0 degreee
    span = 'all' # decav

    for res in grids:
        load_completed_downloads(res)

        for param in parameters:
            for period in time_periods.keys():
                if is_data_downloaded(param, period, res):
                    logger.info(f"Skipping already downloaded dataset: {param} {period} {res}")
                    continue

                padded_period = period.zfill(2)
                file_name = f'woa23_{span}_{param}{padded_period}_{res}.nc'
                url = f'{base_url}/{parameters[param]}/netcdf/{span}/{grid_resolutions[res]}/{file_name}'
                nc_file = os.path.abspath(f'{save_dir}/{file_name}')

                if not os.path.exists(nc_file):
                    logger.info(f"Downloading file from {url}")
                    try:
                        download_data(url, nc_file)
                        write_completed_download(param, period, res, nc_file)
                    except Exception as e:
                        logger.error(f"Skipping file {file_name} due to error: {e}")
                else:
                    logger.info(f"Using existing file: {nc_file}")

if __name__ == '__main__':
    save_dir = os.path.abspath('./netcdf')
    download_woa23_datasets(save_dir)
