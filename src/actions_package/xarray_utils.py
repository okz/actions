"""
Xarray utilities for the actions package.
"""

import xarray as xr
import numpy as np
from typing import Optional, Dict, Any, Iterable
import icechunk.xarray as icx
from zarr.registry import register_codec


def create_sample_dataset() -> xr.Dataset:
    """
    Create a sample xarray Dataset for testing.
    
    Returns:
        xarray Dataset with sample data.
    """
    # Create sample data
    np.random.seed(42)
    time = np.arange(0, 10)
    lat = np.linspace(-90, 90, 5)
    lon = np.linspace(-180, 180, 8)
    
    # Create sample temperature data
    temperature = np.random.randn(len(time), len(lat), len(lon))
    
    # Create Dataset
    ds = xr.Dataset(
        {
            "temperature": (["time", "lat", "lon"], temperature),
        },
        coords={
            "time": time,
            "lat": lat,
            "lon": lon,
        },
        attrs={
            "title": "Sample temperature data",
            "description": "Generated sample data for testing",
        }
    )
    
    return ds


def dataset_info(dataset: xr.Dataset) -> Dict[str, Any]:
    """
    Get basic information about an xarray Dataset.
    
    Args:
        dataset: xarray Dataset to analyze
        
    Returns:
        Dictionary containing dataset information.
    """
    return {
        "dims": dict(dataset.sizes),
        "coords": list(dataset.coords.keys()),
        "data_vars": list(dataset.data_vars.keys()),
        "attrs": dataset.attrs,
        "size": dataset.nbytes,
    }


def save_dataset_to_netcdf(dataset: xr.Dataset, filename: str) -> bool:
    """
    Save xarray Dataset to NetCDF file.
    
    Args:
        dataset: xarray Dataset to save
        filename: Output filename
        
    Returns:
        True if successful, False otherwise.
    """
    try:
        dataset.to_netcdf(filename)
        return True
    except Exception as e:
        print(f"Error saving dataset to NetCDF: {e}")
        return False


def load_dataset_from_netcdf(filename: str) -> Optional[xr.Dataset]:
    """
    Load xarray Dataset from NetCDF file.
    
    Args:
        filename: Input filename
        
    Returns:
        xarray Dataset if successful, None otherwise.
    """
    try:
        return xr.open_dataset(filename)
    except Exception as e:
        print(f"Error loading dataset from NetCDF: {e}")
        return None


def clean_dataset(ds: xr.Dataset) -> xr.Dataset:
    """Return a copy with unused coords dropped and encodings cleared."""
    used_dims = set()
    for var in ds.data_vars:
        used_dims.update(ds[var].dims)
    drop_coords = [c for c in ds.coords if set(ds[c].dims).isdisjoint(used_dims)]
    ds = ds.drop_vars(drop_coords)
    for name in list(ds.variables):
        ds[name].encoding.clear()
        if ds[name].dtype.kind in {"S", "O"}:
            ds[name] = ds[name].astype(str)
            ds[name].encoding.clear()
    return ds


def select_minimal_variables(ds: xr.Dataset) -> xr.Dataset:
    """Return dataset with minimal variables used for base uploads."""
    min_vars = {"json"}
    min_vars.update(v for v in ds.data_vars if "retro" in ds[v].dims)
    min_vars.update(
        [
            "temperature_k",
            "pressure_torr",
            "humidity_percent",
            "signal_strength_dbm",
            "measurement_validity",
            "diagnostics_settings_id",
        ]
    )
    for name in ds.attrs.get("fitted_measurements", "").split():
        min_vars.add(name)
        err = name + "_err"
        if err not in ds:
            err = name + "_stderr"
        if err in ds:
            min_vars.add(err)
    subset = ds[sorted(min_vars)]
    return clean_dataset(subset)


def select_waveform_variables(ds: xr.Dataset, exclude: Iterable[str]) -> xr.Dataset:
    """Variables with ``timestamp`` dimension excluding ``exclude`` and high-res."""
    candidates = [
        v
        for v in ds.data_vars
        if v not in set(exclude)
        and "timestamp" in ds[v].dims
        and "high_res_timestamp" not in ds[v].dims
    ]
    subset = ds[candidates]
    return clean_dataset(subset)


def select_high_freq_variables(ds: xr.Dataset) -> xr.Dataset:
    """Variables that use the ``high_res_timestamp`` dimension."""
    candidates = [v for v in ds.data_vars if "high_res_timestamp" in ds[v].dims]
    subset = ds[candidates]
    return clean_dataset(subset)


def upload_in_intervals(
    repo: "icechunk.Repository",
    ds: xr.Dataset,
    dim: str,
    interval: np.timedelta64,
    mode_first: str = "w",
) -> None:
    """Upload *ds* to *repo* in chunks along *dim* with given *interval*.

    Parameters
    ----------
    repo: icechunk.Repository
        Target repository.
    ds: xr.Dataset
        Dataset slice to upload.
    dim: str
        Dimension along which to chunk the data.
    interval: numpy.timedelta64
        Size of each chunk along *dim*.
    mode_first: str, optional
        Mode for the first chunk. Use ``"w"`` when creating a new repository
        and ``"a"`` when adding new variables to an existing dimension.
    """
    start = ds[dim].values[0]
    end = ds[dim].values[-1]
    first_end = start + interval
    first_slice = ds.sel({dim: slice(start, first_end)})
    session = repo.writable_session("main")
    icx.to_icechunk(first_slice, session, mode=mode_first)
    session.commit("initial chunk")

    current = first_end
    while current < end:
        next_t = current + interval
        chunk = ds.sel({dim: slice(current, next_t)})
        if chunk.sizes.get(dim, 0) > 0:
            session = repo.writable_session("main")
            icx.to_icechunk(chunk, session, mode="a", append_dim=dim)
            session.commit("append chunk")
        current = next_t


def upload_single_chunk(repo: "icechunk.Repository", ds: xr.Dataset, message: str = "single chunk") -> None:
    """Upload the entire dataset to the repository in one commit."""
    session = repo.writable_session("main")
    icx.to_icechunk(ds, session, mode="w")
    session.commit(message)


class _NullCodec:
    codec_id = "null"

    def encode(self, buf):  # type: ignore[override]
        return buf

    def decode(self, buf, out=None):  # type: ignore[override]
        return buf


def ensure_null_codec() -> None:
    """Register a no-op codec under the name ``null`` if missing."""
    try:
        register_codec(_NullCodec())
    except Exception:
        pass

