import xarray as xr
import numpy as np
import pandas as pd
from typing import Optional, List, Union
from pathlib import Path
from zarr.storage import ZipStore


def _open_seed_dataset(path: Union[str, Path]) -> xr.Dataset:
    """Open a dataset from NetCDF or zipped zarr."""
    p = Path(path)
    if p.suffix == ".zip":
        with ZipStore(p, mode="r") as store:
            ds = xr.open_zarr(store)
            ds.load()
        return ds
    return xr.open_dataset(p)


def generate_mock_data(
    seed_file: Union[str, Path],
    output_file: Union[str, Path],
    target_size_mb: Optional[float] = None,
    target_duration_hours: Optional[float] = None,
    additional_retro_ids: Optional[List[int]] = None
) -> xr.Dataset:
    """
    Generate mock data by extending an existing xarray dataset.
    
    Parameters
    ----------
    seed_file : str or Path
        Path to the seed dataset file (NetCDF or zipped Zarr)
    output_file : str or Path
        Path where the generated mock data will be saved
    target_size_mb : float, optional
        Target file size in megabytes. Cannot be used with target_duration_hours.
    target_duration_hours : float, optional
        Target duration in hours to extend the data. Cannot be used with target_size_mb.
    additional_retro_ids : list of int, optional
        List of new retro IDs to add to the dataset
        
    Returns
    -------
    xr.Dataset
        The generated mock dataset
        
    Raises
    ------
    ValueError
        If both target_size_mb and target_duration_hours are provided,
        or if neither is provided
    """
    # Validate input parameters
    if (target_size_mb is None and target_duration_hours is None):
        raise ValueError("Either target_size_mb or target_duration_hours must be provided")
    if (target_size_mb is not None and target_duration_hours is not None):
        raise ValueError("Only one of target_size_mb or target_duration_hours can be provided")
    
    # Load the seed dataset
    ds_seed = _open_seed_dataset(seed_file)
    
    # Calculate the current file size in MB
    current_size_mb = ds_seed.nbytes / (1024 * 1024)
    
    # Extract timestamp information
    timestamps = ds_seed['timestamp'].values
    high_res_timestamps = ds_seed['high_res_timestamp'].values
    
    # Calculate time deltas to use when repeating the dataset
    timestamp_interval = pd.Timedelta(timestamps[1] - timestamps[0])
    high_res_interval = pd.Timedelta(high_res_timestamps[1] - high_res_timestamps[0])
    timestamp_span = pd.Timedelta(timestamps[-1] - timestamps[0]) + timestamp_interval
    high_res_span = (
        pd.Timedelta(high_res_timestamps[-1] - high_res_timestamps[0])
        + high_res_interval
    )
    
    # Determine multiplication factor based on input
    if target_size_mb is not None:
        # Calculate how many times to replicate data based on file size
        multiplication_factor = int(np.ceil(target_size_mb / current_size_mb))
    else:
        # Calculate based on duration
        current_duration = pd.Timedelta(timestamps[-1] - timestamps[0])
        target_duration = pd.Timedelta(hours=target_duration_hours)
        multiplication_factor = int(np.ceil(target_duration / current_duration))
    
    # Create extended timestamps
    extended_timestamps = []
    extended_high_res_timestamps = []
    
    # Generate new timestamps by extending from the last timestamp
    for i in range(multiplication_factor):
        if i == 0:
            # Include original timestamps for the first iteration
            extended_timestamps.extend(timestamps)
            extended_high_res_timestamps.extend(high_res_timestamps)
        else:
            # Offset the timestamps by the span of the dataset so far
            new_timestamps = timestamps + (i * timestamp_span)
            new_high_res_timestamps = high_res_timestamps + (i * high_res_span)

            extended_timestamps.extend(new_timestamps)
            extended_high_res_timestamps.extend(new_high_res_timestamps)
    
    # Create the new dataset with extended dimensions
    new_coords = {}
    new_data_vars = {}
    
    # Handle timestamp-based coordinates and variables
    for coord_name, coord_data in ds_seed.coords.items():
        if 'timestamp' in coord_data.dims and coord_data.dims[0] == 'timestamp':
            # Replicate data for timestamp dimension
            replicated_data = np.tile(coord_data.values, multiplication_factor)
            new_coords[coord_name] = (coord_data.dims, replicated_data)
        elif 'high_res_timestamp' in coord_data.dims and coord_data.dims[0] == 'high_res_timestamp':
            # Replicate data for high_res_timestamp dimension
            replicated_data = np.tile(coord_data.values, multiplication_factor)
            new_coords[coord_name] = (coord_data.dims, replicated_data)
        else:
            # Keep other coordinates as is
            new_coords[coord_name] = coord_data
    
    # Update timestamp coordinates with extended values
    new_coords['timestamp'] = ('timestamp', np.array(extended_timestamps))
    new_coords['high_res_timestamp'] = ('high_res_timestamp', np.array(extended_high_res_timestamps))
    
    # Handle data variables
    for var_name, var_data in ds_seed.data_vars.items():
        if 'timestamp' in var_data.dims:
            replicated_data = np.tile(
                var_data.values,
                tuple(multiplication_factor if d == 'timestamp' else 1 for d in var_data.dims),
            )
            new_data_vars[var_name] = xr.DataArray(
                replicated_data, dims=var_data.dims, attrs=var_data.attrs
            )
        elif 'high_res_timestamp' in var_data.dims:
            replicated_data = np.tile(
                var_data.values,
                tuple(
                    multiplication_factor if d == 'high_res_timestamp' else 1
                    for d in var_data.dims
                ),
            )
            new_data_vars[var_name] = xr.DataArray(
                replicated_data, dims=var_data.dims, attrs=var_data.attrs
            )
        else:
            new_data_vars[var_name] = var_data
    
    # Handle retro dimension expansion if requested
    if additional_retro_ids:
        # Get existing retro data
        existing_retro_ids = ds_seed.coords['retro'].values
        existing_retro_count = len(existing_retro_ids)
        
        # Create new retro IDs
        all_retro_ids = np.concatenate([existing_retro_ids, additional_retro_ids])
        new_coords['retro'] = ('retro', all_retro_ids)
        
        # Extend retro-related coordinates by cycling through existing data
        retro_coords = ['retro_altitude_m', 'retro_latitude', 'retro_longitude', 'retro_name']
        for coord_name in retro_coords:
            if coord_name in ds_seed.coords:
                existing_data = ds_seed.coords[coord_name].values
                # Cycle through existing data for new retro IDs
                indices = np.arange(len(additional_retro_ids)) % existing_retro_count
                new_retro_data = existing_data[indices]
                extended_data = np.concatenate([existing_data, new_retro_data])
                new_coords[coord_name] = ('retro', extended_data)
        
        # Extend retro-related data variables
        for var_name, var_data in new_data_vars.items():
            if 'retro' in var_data.dims:
                retro_axis = var_data.dims.index('retro')
                existing_data = var_data.values

                indices = np.arange(len(additional_retro_ids)) % existing_retro_count
                new_retro_data = np.take(existing_data, indices, axis=retro_axis)

                extended_data = np.concatenate([existing_data, new_retro_data], axis=retro_axis)
                new_data_vars[var_name] = xr.DataArray(
                    extended_data, dims=var_data.dims, attrs=var_data.attrs
                )
    
    # Create the new dataset
    ds_mock = xr.Dataset(
        data_vars=new_data_vars,
        coords=new_coords,
        attrs=ds_seed.attrs.copy()
    )
    
    # Update attributes to reflect mock data generation
    ds_mock.attrs['mock_data'] = "True"
    ds_mock.attrs['mock_multiplication_factor'] = multiplication_factor
    if additional_retro_ids:
        ds_mock.attrs['mock_additional_retro_ids'] = ','.join(map(str, additional_retro_ids))
    
    # Save the dataset
    ds_mock.to_netcdf(output_file)
    
    return ds_mock
