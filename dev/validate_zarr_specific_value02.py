import xarray as xr
import numpy as np

# Load NetCDF data
netcdf_file = '../tmp_data/woa23_decav_t01_04.nc'
nc_ds = xr.open_dataset(netcdf_file, decode_times=False)

# Load Zarr data
data_dir = '../data'
datax = f'{data_dir}/025_degree'
zarr_ds = xr.open_zarr(datax, consolidated=False)

# Select temperature data
param = 'temperature'
time_period = '1'  # Use numeric key as a string

# Ensure the parameters and time periods exist
if param in zarr_ds.parameters.values and time_period in zarr_ds.time_periods.values:
    # Extract data
    netcdf_data = nc_ds['t_an']  # Assuming 't_an' is the temperature variable in NetCDF
    zarr_data = zarr_ds.sel(parameters=param, time_periods=time_period)['an']

    # Interpolate NetCDF data to match Zarr coordinates
    netcdf_data_interp = netcdf_data.interp(lon=zarr_ds['lon'], lat=zarr_ds['lat'], depth=zarr_ds['depth'])

    # Mask NaN values
    netcdf_values = netcdf_data_interp.values.squeeze()
    zarr_values = zarr_data.values

    # Create a mask for non-NaN values in both NetCDF and Zarr datasets
    valid_mask = np.isfinite(netcdf_values) & np.isfinite(zarr_values)

    # Apply the mask to both datasets
    netcdf_values_masked = np.where(valid_mask, netcdf_values, np.nan)
    zarr_values_masked = np.where(valid_mask, zarr_values, np.nan)

    # Find differences excluding NaN values
    diff_mask = (netcdf_values_masked != zarr_values_masked) & valid_mask

    if np.any(diff_mask):
        diff_indices = np.where(diff_mask)
        print("Differences found at indices:", diff_indices)
        
        # Inspecting a smaller subset of the differences
        for idx in range(min(10, len(diff_indices[0]))):  # Only show the first 10 differences
            d, la, lo = diff_indices[0][idx], diff_indices[1][idx], diff_indices[2][idx]
            print(f"Index (depth: {d}, lat: {la}, lon: {lo}) - NetCDF value: {netcdf_values[d, la, lo]}, Zarr value: {zarr_values[d, la, lo]}")
    else:
        print("No differences found between NetCDF and Zarr datasets.")
else:
    print("Parameter or time period not found in the Zarr dataset.")

