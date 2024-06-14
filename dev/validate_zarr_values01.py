import os
import random
import logging
import numpy as np
import xarray as xr

# Set up logging
logging.basicConfig(level=logging.INFO, filename='validation.log', filemode='w',
                    format='%(asctime)s - %(levelname)s - %(message)s')
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console.setFormatter(formatter)
logging.getLogger().addHandler(console)

# Define paths and parameters
data_dir = '../data'
save_dir = '../tmp_data'
zarr_group_path = f'{data_dir}/test'
parameters = ['t', 's']
time_periods = ['1', '2', '3']
data_variables = ['an', 'mn']
grid_resolutions = {'04': '0.25'}

# Define marine regions for random selection
regions = {
    'Pacific': {'lon': (-160, -100), 'lat': (-20, 20)},
    'Atlantic': {'lon': (-60, 20), 'lat': (-20, 20)},
    'WestPacific': {'lon': (125, 155), 'lat': (15, 30)},
    'IndiaOcean': {'lon': (60, 90), 'lat': (-25, 0)}
}

# Function to download data (replace this with your actual download implementation)
def download_data(url, save_path):
    import requests
    response = requests.get(url)
    with open(save_path, 'wb') as f:
        f.write(response.content)

# Function to get actual oceanic coordinates within the specified region from NetCDF
def get_actual_coords(ds, region, lon_range, lat_range, depth_range):
    lon_min, lon_max = region['lon']
    lat_min, lat_max = region['lat']
    
    lons = ds['lon'].values
    lats = ds['lat'].values
    depths = ds['depth'].values
    
    valid_lons = lons[(lons >= lon_min) & (lons <= lon_max)]
    valid_lats = lats[(lats >= lat_min) & (lats <= lat_max)]
    valid_depths = depths[(depths >= 0) & (depths <= 1500)]
    
    if len(valid_lons) == 0 or len(valid_lats) == 0 or len(valid_depths) == 0:
        raise ValueError("No valid coordinates found in the specified region.")
    
    lon_idx = random.randint(0, len(valid_lons) - lon_range)
    lat_idx = random.randint(0, len(valid_lats) - lat_range)
    depth_idx = random.randint(0, len(valid_depths) - depth_range)
    
    lon = valid_lons[lon_idx:lon_idx + lon_range]
    lat = valid_lats[lat_idx:lat_idx + lat_range]
    depth = valid_depths[depth_idx:depth_idx + depth_range]
    
    return lon, lat, depth

# Function to compare values between NetCDF and Zarr
def compare_values(nc_file, zarr_ds, param_key, period_key, lon, lat, depth):
    try:
        logging.info(f"Comparing values for {param_key} during period {period_key} at selected range...")
        
        # Load the new dataset from NetCDF with decode_times=False
        ds_new = xr.open_dataset(nc_file, decode_times=False)

        # Select the nearest coordinates from the NetCDF dataset
        ds_new = ds_new.sel(lon=lon, lat=lat, depth=depth, method='nearest')

        # Compare values for each variable
        for var in data_variables:
            if var in ds_new:
                nc_values = ds_new[var].values
                zarr_values = zarr_ds[var].sel(lon=lon, lat=lat, depth=depth, parameters=param_key, time_periods=period_key, method='nearest').values

                if not np.allclose(nc_values, zarr_values, equal_nan=True):
                    logging.error(f"Mismatch found for {param_key} during period {period_key} at (lon, lat, depth)=({lon}, {lat}, {depth}) "
                                  f"for variable {var}: NetCDF values={nc_values}, Zarr values={zarr_values}")
                    return False

        logging.info(f"Comparison passed for {param_key} during period {period_key} at (lon, lat, depth)=({lon}, {lat}, {depth})")
        return True

    except Exception as e:
        logging.error(f"Error comparing values for {param_key} during period {period_key}: {e}")
        return False

def main():
    iterations = 3
    lon_range = 4
    lat_range = 4
    depth_range = 20

    # Load the Zarr store
    zarr_ds = xr.open_zarr(zarr_group_path, consolidated=False)

    # Outer loop for iterations
    for i in range(iterations):
        logging.info(f"Starting iteration {i+1}/{iterations}...")

        # Iterate through parameters and time periods
        for param in parameters:
            for period in time_periods:
                # Randomly select a region
                region_name, region = random.choice(list(regions.items()))

                # Construct NetCDF file path
                padded_period = period.zfill(2)
                file_name = f'woa23_decav_{param}{padded_period}_04.nc'
                nc_file = f'{save_dir}/{file_name}'

                # Check if the file exists
                if not os.path.exists(nc_file):
                    logging.info(f"Downloading file: {nc_file}")
                    download_data(f'{base_url}/{parameters[param]}/netcdf/decav/{grid_resolutions["04"]}/{file_name}', nc_file)

                # Load the NetCDF dataset to get actual coordinates
                ds_new = xr.open_dataset(nc_file, decode_times=False)
                lon, lat, depth = get_actual_coords(ds_new, region, lon_range, lat_range, depth_range)

                result = compare_values(nc_file, zarr_ds, param, period, lon, lat, depth)
                if not result:
                    logging.error(f"Comparison failed for {param} during period {period} in iteration {i+1}/{iterations}.")

if __name__ == '__main__':
    main()
