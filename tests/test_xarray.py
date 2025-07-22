"""
Tests for xarray utilities.
"""

import pytest
import numpy as np
import tempfile
import os
from actions_package.xarray_utils import (
    create_sample_dataset,
    dataset_info,
    save_dataset_to_netcdf,
    load_dataset_from_netcdf,
)


def test_xarray_import():
    """Test that xarray can be imported."""
    import xarray as xr
    assert xr is not None


def test_create_sample_dataset():
    """Test creating a sample xarray dataset."""
    ds = create_sample_dataset()
    
    # Check that it's an xarray Dataset
    import xarray as xr
    assert isinstance(ds, xr.Dataset)
    
    # Check dimensions
    assert "time" in ds.dims
    assert "lat" in ds.dims
    assert "lon" in ds.dims
    
    # Check data variables
    assert "temperature" in ds.data_vars
    
    # Check coordinates
    assert "time" in ds.coords
    assert "lat" in ds.coords
    assert "lon" in ds.coords
    
    # Check attributes
    assert "title" in ds.attrs
    assert "description" in ds.attrs


def test_dataset_info():
    """Test getting dataset information."""
    ds = create_sample_dataset()
    info = dataset_info(ds)
    
    # Check info structure
    assert "dims" in info
    assert "coords" in info
    assert "data_vars" in info
    assert "attrs" in info
    assert "size" in info
    
    # Check specific values
    assert info["dims"]["time"] == 10
    assert info["dims"]["lat"] == 5
    assert info["dims"]["lon"] == 8
    assert "temperature" in info["data_vars"]
    assert "time" in info["coords"]
    assert "lat" in info["coords"]
    assert "lon" in info["coords"]


def test_save_and_load_netcdf():
    """Test saving and loading dataset to/from NetCDF."""
    ds = create_sample_dataset()
    
    # Create temporary file
    with tempfile.NamedTemporaryFile(suffix='.nc', delete=False) as tmp:
        temp_path = tmp.name
    
    try:
        # Test saving
        result = save_dataset_to_netcdf(ds, temp_path)
        assert result is True
        assert os.path.exists(temp_path)
        
        # Test loading
        loaded_ds = load_dataset_from_netcdf(temp_path)
        assert loaded_ds is not None
        
        # Check that loaded dataset has same structure
        assert list(loaded_ds.sizes.keys()) == list(ds.sizes.keys())
        assert list(loaded_ds.data_vars.keys()) == list(ds.data_vars.keys())
        assert list(loaded_ds.coords.keys()) == list(ds.coords.keys())
        
        # Check data values are close (accounting for floating point precision)
        np.testing.assert_array_almost_equal(
            loaded_ds.temperature.values,
            ds.temperature.values
        )
        
    finally:
        # Clean up
        if os.path.exists(temp_path):
            os.unlink(temp_path)


def test_load_nonexistent_netcdf():
    """Test loading from non-existent NetCDF file."""
    result = load_dataset_from_netcdf("/nonexistent/file.nc")
    assert result is None


def test_save_to_invalid_path():
    """Test saving to invalid path."""
    ds = create_sample_dataset()
    # Use a read-only system path so the save operation fails regardless of permissions
    result = save_dataset_to_netcdf(ds, "/proc/invalid/path/file.nc")
    assert result is False


class TestXarrayOperations:
    """Test class for xarray operations."""
    
    def test_dataset_arithmetic(self):
        """Test basic arithmetic operations on dataset."""
        ds = create_sample_dataset()
        
        # Test arithmetic operations
        ds_doubled = ds * 2
        assert "temperature" in ds_doubled.data_vars
        
        # Check that values are doubled
        np.testing.assert_array_almost_equal(
            ds_doubled.temperature.values,
            ds.temperature.values * 2
        )
    
    def test_dataset_selection(self):
        """Test selecting data from dataset."""
        ds = create_sample_dataset()
        
        # Test time selection
        ds_subset = ds.isel(time=slice(0, 5))
        assert ds_subset.sizes["time"] == 5
        
        # Test coordinate selection
        ds_subset = ds.sel(lat=slice(-45, 45))
        assert ds_subset.sizes["lat"] <= ds.sizes["lat"]
    
    def test_dataset_aggregation(self):
        """Test aggregation operations on dataset."""
        ds = create_sample_dataset()
        
        # Test mean calculation
        mean_temp = ds.temperature.mean()
        assert isinstance(mean_temp.values.item(), (np.floating, float))
        
        # Test mean over specific dimension
        time_mean = ds.temperature.mean(dim="time")
        assert time_mean.shape == (ds.sizes["lat"], ds.sizes["lon"])


@pytest.mark.parametrize("time_length,lat_length,lon_length", [
    (5, 3, 4),
    (10, 5, 8),
    (15, 7, 12),
])
def test_dataset_creation_parametrized(time_length, lat_length, lon_length):
    """Parametrized test for dataset creation with different dimensions."""
    # Create custom dataset
    import xarray as xr
    np.random.seed(42)
    
    time = np.arange(0, time_length)
    lat = np.linspace(-90, 90, lat_length)
    lon = np.linspace(-180, 180, lon_length)
    
    temperature = np.random.randn(time_length, lat_length, lon_length)
    
    ds = xr.Dataset(
        {"temperature": (["time", "lat", "lon"], temperature)},
        coords={"time": time, "lat": lat, "lon": lon},
    )
    
    # Test dimensions
    assert ds.sizes["time"] == time_length
    assert ds.sizes["lat"] == lat_length
    assert ds.sizes["lon"] == lon_length
    
    # Test data variable
    assert "temperature" in ds.data_vars
    assert ds.temperature.shape == (time_length, lat_length, lon_length)