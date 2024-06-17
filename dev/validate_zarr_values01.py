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
grid_resolutions = {'04': '0.25', '01': '1.00'}
grid_dir = {'04': '025_degree', '01': '1_degree'}
grid_res = '04'  # change this to '01' for 1-degree resolution, '04' for 0.25-degree
zarr_group_path = f'{data_dir}/{grid_dir[grid_res]}/'
param_keys = ['t', 's'] # ['t', 's', 'o', 'O', 'A', 'i', 'p', 'n']  # for 0.25-degree
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
time_periods = ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15', '16'] 
data_variables = ['an', 'mn']

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

# Function to determine the subgroup
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

    return subgroup

# Function to get actual oceanic coordinates within the specified region from NetCDF
def get_actual_coords(ds, region, lon_range, lat_range, depth_levels):
    lon_min, lon_max = region['lon']
    lat_min, lat_max = region['lat']

    lons = ds['lon'].values
    lats = ds['lat'].values
    depths = ds['depth'].values

    valid_lons = lons[(lons >= lon_min) & (lons <= lon_max)]
    valid_lats = lats[(lats >= lat_min) & (lats <= lat_max)]
    valid_depths = depths[:depth_levels]

    if len(valid_lons) == 0 or len(valid_lats) == 0 or len(valid_depths) == 0:
        raise ValueError("No valid coordinates found in the specified region.")

    lon_idx = random.randint(0, len(valid_lons) - lon_range)
    lat_idx = random.randint(0, len(valid_lats) - lat_range)
    depth_idx = random.randint(0, len(valid_depths) - len(valid_depths))

    lon = valid_lons[lon_idx:lon_idx + lon_range]
    lat = valid_lats[lat_idx:lat_idx + lat_range]
    depth = valid_depths

    return lon, lat, depth

# Function to determine the depth levels based on the subgroup
def get_depth_levels(subgroup):
    if 'annual' in subgroup or ('TS' in subgroup and not 'monthly' in subgroup):
        return 102
    elif 'Oxy' in subgroup or 'TS' in subgroup:
        return 57
    else:
        return 43
    
# Function to compare values between NetCDF and Zarr
def compare_values(nc_file, zarr_ds, param_key, period_key, lon, lat, depth, run):
    try:
        logging.info(f"Comparing {data_variables} for {param_key} during period {period_key} at selected range...")

        # Load the new dataset from NetCDF with decode_times=False
        ds_new = xr.open_dataset(nc_file, decode_times=False)

        # Select the nearest coordinates from the NetCDF dataset
        ds_new = ds_new.sel(lon=lon, lat=lat, depth=depth, method='nearest')

        # Log available parameters
        # logging.info(f"Available parameters in NetCDF: {list(ds_new.keys())}")
        # logging.info(f"Available parameters in Zarr: {list(zarr_ds.keys())}")
        result = False
        
        # Compare values for each variable
        for var in data_variables:
            netcdf_var = f"{param_key}_{var}"
            if netcdf_var in ds_new:
                logging.info(f"Comparing variable {netcdf_var} in NetCDF with {var} in Zarr")
                nc_values = ds_new[netcdf_var].values
                if (run == 1 and param_key == 't' and period_key == '0' and var == 'mn'): 
                    logging.info(f"NetCDF values: {nc_values}")

                zarr_values = zarr_ds[var].sel(lon=lon, lat=lat, depth=depth, parameters=parameters[param_key], time_periods=period_key).values

                # Log specific values for debugging
                if (run == 1 and param_key == 't' and period_key == '0' and var == 'mn'): 
                    logging.info(f"Zarr values: {zarr_values}")

                # Ensure the values are numerical arrays
                if isinstance(nc_values, np.ndarray) and isinstance(zarr_values, np.ndarray):
                    if not np.allclose(nc_values, zarr_values, equal_nan=True):
                        logging.error(f"Mismatch found for {parameters[param_key]} during period {period_key} at (lon, lat, depth)=({lon}, {lat}, {depth}) "
                                      f"for variable {var}: NetCDF values={nc_values}, Zarr values={zarr_values}")
                        result = False
                    else:
                        logging.info(f"All matches while comparing NetCDF with Zarr values")
                        result = True
                        
                else:
                    logging.error(f"Non-numerical values encountered: NetCDF values type={type(nc_values)}, Zarr values type={type(zarr_values)}")
                    result = False

        if not result:
            logging.info(f"{run}. Comparison failed for {parameters[param_key]} during period {period_key} at (lon, lat, depth)=({lon}, {lat}, {depth})")

        logging.info(f"{run}. Comparison passed for {parameters[param_key]} during period {period_key} at (lon, lat, depth)=({lon}, {lat}, {depth})")
        return True

    except Exception as e:
        logging.error(f"Error comparing values for {parameters[param_key]} during period {period_key} at (lon, lat, depth)=({lon}, {lat}, {depth}): {e}")
        return False

def main():
    iterations = 3
    lon_range = 4
    lat_range = 4

    for param in param_keys:
        for period in time_periods:
            subgroup = determine_subgroup(param, period)
            depth_levels = get_depth_levels(subgroup)
            zarr_ds_path = os.path.join(data_dir, grid_dir[grid_res], subgroup)
            zarr_ds = xr.open_zarr(zarr_ds_path, consolidated=False)

            for i in range(iterations):
                logging.info(f"Starting iteration {i+1}/{iterations}...")

                # Randomly select a region
                region_name, region = random.choice(list(regions.items()))

                # Construct NetCDF file path
                padded_period = period.zfill(2)
                span = 'decav' if param in ['t', 's'] else 'all'
                base_url = "https://www.ncei.noaa.gov/data/oceans/woa/WOA23/DATA" if param in ['t', 's'] else "https://www.ncei.noaa.gov/thredds-ocean/fileServer/woa23/DATA"
                file_name = f'woa23_{span}_{param}{padded_period}_{grid_res}.nc'
                nc_file = f'{save_dir}/{file_name}'

                # Check if the file exists
                if not os.path.exists(nc_file):
                    logging.info(f"Downloading file: {nc_file}")
                    download_data(f'{base_url}/{parameters[param]}/netcdf/{span}/{grid_resolutions[grid_res]}/{file_name}', nc_file)

                # Load the NetCDF dataset to get actual coordinates
                ds_new = xr.open_dataset(nc_file, decode_times=False)
                lon, lat, depth = get_actual_coords(ds_new, region, lon_range, lat_range, depth_levels)

                result = compare_values(nc_file, zarr_ds, param, period, lon, lat, depth, i+1)
                if not result:
                    logging.error(f"Comparison failed for {param} during period {period} in iteration {i+1}/{iterations}.")


if __name__ == '__main__':
    main()
