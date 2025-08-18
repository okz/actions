import os
import xarray as xr
import numpy as np
import pandas as pd
from typing import Optional, List, Union
from pathlib import Path
from zarr.storage import ZipStore
import icechunk

from .blocks import upload_single_chunk


# Compressor spec used for generated mock data and icechunk uploads
DEFAULT_COMPRESSOR = {
    "name": "blosc",
    "configuration": {"cname": "zstd", "clevel": 3},
}


def _open_seed_dataset(path: Union[str, Path]) -> xr.Dataset:
    """Open a dataset from NetCDF or zipped zarr."""
    p = Path(path)
    if p.suffix == ".zip":
        with ZipStore(p, mode="r") as store:
            ds = xr.open_zarr(store)
            ds.load()
        return ds
    return xr.open_dataset(p)


def build_blob_base_path(ds: xr.Dataset, base: Optional[Union[str, Path]] = None) -> Path:
    """Return backup base path ``<base>/<instrument>/<project>`` for *ds*.

    Parameters
    ----------
    ds : xr.Dataset
        Dataset containing ``instrument`` and ``project`` attributes.
    base : str or Path, optional
        Override for ``CLADS_BACKUP_UPLOAD_TARGET`` environment variable.
    """
    root = Path(base or os.environ["CLADS_BACKUP_UPLOAD_TARGET"])
    instrument = ds.attrs.get("instrument", "")
    project = ds.attrs.get("project", "")
    return root / instrument / project


def generate_ice_chunk_repositories(
    seed_file: Union[str, Path],
    count: int = 1,
    base: Optional[Union[str, Path]] = None,
) -> List[Path]:
    """Create *count* icechunk repositories from *seed_file*.

    Repositories are placed under ``<base>/<instrument>/<project>`` where
    *base* defaults to the ``CLADS_BACKUP_UPLOAD_TARGET`` environment variable.
    Each repository name follows the pattern
    ``inst-<instrument>-prj-<project>-<YYYY-MM-DDtHH-mm-SSz>l1b`` and a unique
    timestamp is generated for every repository created.
    """

    ds_seed = _open_seed_dataset(seed_file)

    paths: List[Path] = []
    base_ts = np.datetime64(ds_seed["timestamp"].values[0], "s")

    for i in range(count):
        ds = ds_seed.copy(deep=True)
        for var in ds.variables:
            ds[var].encoding.clear()
            if ds[var].dtype.kind not in {"O", "S"}:
                ds[var].encoding["compressors"] = [DEFAULT_COMPRESSOR]

        repo_base = build_blob_base_path(ds, base)
        repo_base.mkdir(parents=True, exist_ok=True)

        ts = base_ts + np.timedelta64(i, "s")
        ts_str = (
            np.datetime_as_string(ts, unit="s")
            .replace("T", "t")
            .replace(":", "-")
            + "z"
        )
        instrument = ds.attrs.get("instrument", "")
        project = ds.attrs.get("project", "")
        repo_name = f"inst-{instrument}-prj-{project}-{ts_str}l1b"
        repo_path = repo_base / repo_name

        storage = icechunk.local_filesystem_storage(str(repo_path))
        repo = icechunk.Repository.create(storage)
        upload_single_chunk(repo, ds)

        paths.append(repo_path)

    return paths


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
    
    # Load the seed dataset and ensure chronological order for time-based
    # dimensions. Some test fixtures ship with unsorted coordinates which can
    # cause slice operations using ``xarray``/``pandas`` to raise ``KeyError``.
    # Sorting provides a stable base for our replication logic.
    ds_seed = _open_seed_dataset(seed_file)
    if 'high_res_timestamp' in ds_seed.coords:
        ds_seed = ds_seed.sortby('high_res_timestamp')
        hrs = ds_seed['high_res_timestamp'].values.copy()
        for i in range(1, len(hrs)):
            if hrs[i] <= hrs[i - 1]:
                hrs[i] = hrs[i - 1] + np.timedelta64(1, 'ns')
        ds_seed = ds_seed.assign_coords(high_res_timestamp=('high_res_timestamp', hrs))
    if 'timestamp' in ds_seed.coords:
        ds_seed = ds_seed.sortby('timestamp')

    # Calculate the current file size in MB based on the on-disk size of the
    # source file instead of the in-memory representation. Using ``nbytes``
    # can significantly overestimate the original data size because it reports
    # the fully decompressed array sizes. This led to multiplication factors of
    # ``1`` when the seed dataset was already smaller than ``target_size_mb``,
    # so the generated mock data contained no additional samples. Measuring the
    # actual file size ensures the dataset is extended whenever the target size
    # exceeds the source file.
    seed_path = Path(seed_file)
    current_size_mb = seed_path.stat().st_size / (1024 * 1024)
    
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
        # Calculate how many times to replicate data based on file size.
        # Ensure at least one repetition so that the generated dataset always
        # contains new samples even when the target size is smaller than the
        # original file.
        multiplication_factor = int(np.ceil(target_size_mb / current_size_mb))
        multiplication_factor = max(2, multiplication_factor)
    else:
        # Calculate based on duration and similarly guarantee at least one
        # repetition when the requested duration does not exceed the seed.
        current_duration = pd.Timedelta(timestamps[-1] - timestamps[0])
        target_duration = pd.Timedelta(hours=target_duration_hours)
        multiplication_factor = int(np.ceil(target_duration / current_duration))
        multiplication_factor = max(2, multiplication_factor)
    
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
            arr = xr.DataArray(
                replicated_data, dims=var_data.dims, attrs=var_data.attrs
            )
            arr.encoding["compressors"] = [DEFAULT_COMPRESSOR]
            new_data_vars[var_name] = arr
        elif 'high_res_timestamp' in var_data.dims:
            replicated_data = np.tile(
                var_data.values,
                tuple(
                    multiplication_factor if d == 'high_res_timestamp' else 1
                    for d in var_data.dims
                ),
            )
            arr = xr.DataArray(
                replicated_data, dims=var_data.dims, attrs=var_data.attrs
            )
            arr.encoding["compressors"] = [DEFAULT_COMPRESSOR]
            new_data_vars[var_name] = arr
        else:
            arr = var_data.copy()
            arr.encoding["compressors"] = [DEFAULT_COMPRESSOR]
            new_data_vars[var_name] = arr
    
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
                arr = xr.DataArray(
                    extended_data, dims=var_data.dims, attrs=var_data.attrs
                )
                if arr.dtype.kind not in {"O", "S"}:
                    arr.encoding["compressors"] = [DEFAULT_COMPRESSOR]
                new_data_vars[var_name] = arr
    
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
    
    # Save the dataset with netCDF compression
    nc_encoding: dict[str, dict[str, int | bool]] = {}
    for name, var in ds_mock.variables.items():
        if var.dtype.kind not in {"O", "S"}:
            nc_encoding[name] = {"zlib": True, "complevel": 3}
    ds_mock.to_netcdf(output_file, encoding=nc_encoding)
    
    return ds_mock
