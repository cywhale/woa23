import os
import logging
import xarray as xr
import numpy as np
from concurrent.futures import ProcessPoolExecutor, as_completed
import dask.array as da
from dask import delayed
from dask.diagnostics import ProgressBar
import dask
import zarr
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from tqdm import tqdm

# Define paths and parameters
parameters = {
    't': 'temperature',
    's': 'salinity',
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
data_variables = ['an', 'mn', 'dd', 'ma', 'sd', 'se', 'oa', 'gp', 'sdo', 'sea']
chunk_sizes = {'time_periods': 1, 'parameters': 1, 'depth': 8, 'lat': 90, 'lon': 360}
grid_resolutions = {
    '01': '1.00',
    '04': '0.25'
}
grid_dir = {
    '01': '1_degree',
    '04': '025_degree'
}

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()
if not logger.hasHandlers():
    file_handler = logging.FileHandler('processing.log', mode='w')
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levellevel)s - %(message)s'))
    logger.addHandler(file_handler)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levellevel)s - %(message)s'))
    logger.addHandler(console_handler)

completed_datasets = set()

# Function to read completed datasets
def load_completed_datasets(res):
    global completed_datasets
    completed_data_file = f'data_completed_{res}.txt'
    if os.path.exists(completed_data_file):
        with open(completed_data_file, 'r') as f:
            for line in f:
                parts = line.strip().split(',')
                completed_datasets.add(f"{parts[0]},{parts[1]},{parts[2]}")

# Function to write a completed dataset entry
def write_completed_dataset(param, period, grid_res, nc_file):
    completed_data_file = f'data_completed_{grid_res}.txt'
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


# Function to initialize Zarr store with empty data variables
def initialize_zarr_store(zarr_group_path, ds, chunk_sizes):
    try:
        zarr.open_group(zarr_group_path, mode='r')
        logger.info(f"Zarr store already initialized at {zarr_group_path}. Skipping initialization.")
        return
    except zarr.errors.GroupNotFoundError:
        logger.info("Initializing Zarr store...")
        empty_ds = xr.Dataset(coords=ds.coords)
        empty_ds.to_zarr(zarr_group_path, mode='w')

        # Incrementally add each variable to avoid memory exhaustion
        for var in data_variables:
            if var == 'ma' and '0' in ds.coords['time_periods']:
                continue
            if var == 'sdo' and ('Nutrients' in zarr_group_path or ('Oxy' in zarr_group_path and not 'annual' in zarr_group_path)):
                continue
            data_shape = (
                len(ds.coords['time_periods']),
                len(ds.coords['parameters']),
                len(ds.coords['depth']),
                len(ds.coords['lat']),
                len(ds.coords['lon'])
            )
            temp_data = np.empty(data_shape, dtype=np.float32)
            temp_data.fill(np.nan)  # Fill with NaNs to indicate empty data
            temp_ds = xr.Dataset({var: (('time_periods', 'parameters', 'depth', 'lat', 'lon'), temp_data)}, coords=ds.coords)
            temp_ds = temp_ds.chunk(chunk_sizes)
            temp_ds.to_zarr(zarr_group_path, mode='a')
            del temp_data, temp_ds  # Clear variables to free up memory
        logger.info("Zarr store initialized.")

# Function to append a new dataset to the existing Zarr store using Dask
@delayed
def append_to_zarr_store(zarr_group_path, nc_file, param_key, period_key, grid_res):
    try:
        logger.info(f"Appending {param_key} for period {period_key} from {nc_file}...")

        # Open the existing Zarr store
        ds_existing = xr.open_zarr(zarr_group_path, consolidated=False)
        
        # Load the new dataset from NetCDF with decode_times=False
        ds_new = xr.open_dataset(nc_file, decode_times=False)

        # Drop unused variables if they exist
        ds_new = ds_new.drop_vars(['crs', 'lat_bnds', 'lon_bnds', 'depth_bnds', 'climatology_bounds'], errors='ignore')
        
        # Rename variables according to data_variables
        rename_vars = {f'{param_key}_{var}': var for var in data_variables if not ((var == 'ma' and period_key == '0') or (var == 'sdo' and ('Nutrients' in zarr_group_path or ('Oxy' in zarr_group_path and not 'annual' in zarr_group_path))))}
        ds_new = ds_new.rename(rename_vars)
        
        # Remove the time dimension if it exists
        for var in ds_new.data_vars:
            if 'time' in ds_new[var].dims:
                ds_new[var] = ds_new[var].squeeze('time', drop=True)
        ds_new = ds_new.drop_vars('time', errors='ignore')

        # Extract the parameter name
        param_name = parameters[param_key]

        # Ensure the new dataset has the correct coordinates and dimensions
        ds_new = ds_new.expand_dims({'parameters': [param_name], 'time_periods': [period_key]})

        for var in ds_new.data_vars:
            # Extract the relevant data for the region update
            new_data = ds_new[var].data.squeeze()
            
            # Create a temporary DataArray with the correct dimensions
            temp_da = xr.DataArray(
                new_data,
                dims=['depth', 'lat', 'lon'],
                coords={
                    'depth': ds_existing.depth,
                    'lat': ds_existing.lat,
                    'lon': ds_existing.lon
                }
            )
            
            # Update the existing dataset with the new data in the specified region
            ds_existing[var].loc[dict(parameters=param_name, time_periods=period_key)] = temp_da

        # Write the updated data to the Zarr store without re-writing the coordinates
        ds_existing.chunk(chunk_sizes).to_zarr(zarr_group_path, mode='a')
        logger.info(f"Successfully appended {param_key} for period {period_key}.")

        # Log the completion of this dataset
        write_completed_dataset(param_key, period_key, grid_res, nc_file)

    except Exception as e:
        logger.error(f"Error appending {param_key} for period {period_key}: {e}")
        raise

# Function to check if a dataset has been completed
def is_data_completed(param, period, grid_res):
    return f'{param},{period},{grid_res}' in completed_datasets

# Function to determine the subgroup based on parameter and period
def determine_subgroup(param, period):
    param_name = parameters[param]
    param_group = 'Nutrients'

    if param_name in ['temperature', 'salinity']:
        param_group = 'TS'
    elif param_name in ['oxygen', 'o2sat', 'AOU']:
        param_group = 'Oxy'
    
    if period == '0':
        subgroup = f'annual/{param_group}'
    elif period in ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12']:
        subgroup = f'monthly/{param_group}'
    else:
        subgroup = f'seasonal/{param_group}'    

    logger.info(f"current handling {param} for period {period}. Get {param_name} and {subgroup}")
    return subgroup

# Top-level function for parallel processing
def download_and_process(param, period, save_dir, data_dir, res, chunk_sizes):
    if is_data_completed(param, period, res):
        logger.info(f"Skipping already completed dataset: {param} {period} {res}")
        return

    span = 'decav' if param in ['t', 's'] else 'all'
    base_url = "https://www.ncei.noaa.gov/data/oceans/woa/WOA23/DATA" if param in ['t', 's'] else "https://www.ncei.noaa.gov/thredds-ocean/fileServer/woa23/DATA"
    padded_period = period.zfill(2)
    file_name = f'woa23_{span}_{param}{padded_period}_{res}.nc'
    url = f'{base_url}/{parameters[param]}/netcdf/{span}/{grid_resolutions[res]}/{file_name}'
    nc_file = os.path.abspath(f'{save_dir}/{file_name}')

    if not os.path.exists(nc_file):
        logger.info(f"Downloading file: {nc_file}")
        download_data(url, nc_file)
    else:
        logger.info(f"Using existing file: {nc_file}")

    subgroup = determine_subgroup(param, period)
    zarr_group_path = os.path.abspath(f'{data_dir}/{grid_dir[res]}/{subgroup}')
    
    lon = np.arange(-179.875, 180, 0.25, dtype=np.float32) if res == '04' else np.arange(-179.5, 180, 1.0, dtype=np.float32)
    lat = np.arange(-89.875, 90, 0.25, dtype=np.float32) if res == '04' else np.arange(-89.5, 90, 1.0, dtype=np.float32)
 
    if 'annual' in subgroup or ('TS' in subgroup and not 'monthly' in subgroup):
        depth = np.concatenate([np.arange(0, 100, 5), np.arange(100, 500, 25), np.arange(500, 800, 50), np.arange(800, 2000, 50), np.arange(2000, 5600, 100)], dtype=np.float32)
    elif 'Oxy' in subgroup or 'TS' in subgroup:
        depth = np.concatenate([np.arange(0, 100, 5), np.arange(100, 500, 25), np.arange(500, 800, 50), np.arange(800, 1550, 50)], dtype=np.float32)
    else:
        depth = np.concatenate([np.arange(0, 100, 5), np.arange(100, 500, 25), np.arange(500, 850, 50)], dtype=np.float32)
    
    # Ensure the correct parameters and time periods for each subgroup
    subgroup_parameters = ['temperature', 'salinity']
    if 'Oxy' in subgroup:
        subgroup_parameters = ['oxygen', 'o2sat', 'AOU']
    elif 'Nutrients' in subgroup:
        subgroup_parameters = ['nitrate', 'phosphate', 'silicate']

    subgroup_time_periods = ['0']
    if 'monthly' in subgroup:
        subgroup_time_periods = ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12']
    elif 'seasonal' in subgroup:
        subgroup_time_periods = ['13', '14', '15', '16']

    ds_initial = xr.Dataset(
        coords={
            'lon': lon,
            'lat': lat,
            'depth': depth,
            'parameters': subgroup_parameters,
            'time_periods': subgroup_time_periods
        }
    )
    
    initialize_zarr_store(zarr_group_path, ds_initial, chunk_sizes)
    return append_to_zarr_store(zarr_group_path, nc_file, param, period, res)

# Main processing function
def process_subgroup(save_dir, data_dir, res):
    params = ['t', 's', 'o', 'O', 'A', 'i', 'p', 'n']
    periods = list(time_periods) # ['0', '1', '2', '3', '4', '5', '6', '7', '13', '14', '15', '16']  # Example for trials

    delayed_tasks = []
    for param in params:
        for period in periods:
            delayed_tasks.append(download_and_process(param, period, save_dir, data_dir, res, chunk_sizes))

    with ProgressBar():
        dask.compute(*delayed_tasks, num_workers=4)

# Create the initial empty Zarr store
def main():
    data_dir = os.path.abspath('../data')
    save_dir = os.path.abspath('../tmp_data')
    res = '01'  # change this to '01' for 1-degree resolution, '04' for 0.25-degree

    load_completed_datasets(res)
    process_subgroup(save_dir, data_dir, res)

if __name__ == '__main__':
    main()
