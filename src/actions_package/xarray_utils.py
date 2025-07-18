"""
Xarray utilities for the actions package.
"""

import xarray as xr
import numpy as np
from typing import Optional, Dict, Any


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