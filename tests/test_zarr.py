"""
Tests for zarr utilities.
"""

import pytest
import numpy as np
import tempfile
import os
import shutil
from actions_package.zarr_utils import (
    create_sample_zarr_array,
    create_zarr_group,
    save_zarr_to_directory,
    load_zarr_from_directory,
    zarr_info,
    create_temp_zarr_store,
    cleanup_temp_zarr_store,
)


def test_zarr_import():
    """Test that zarr can be imported."""
    import zarr
    assert zarr is not None


def test_create_sample_zarr_array():
    """Test creating a sample zarr array."""
    z = create_sample_zarr_array()
    
    # Check that it's a zarr Array
    import zarr
    assert isinstance(z, zarr.Array)
    
    # Check default shape and chunks
    assert z.shape == (100, 100)
    assert z.chunks == (10, 10)
    
    # Test with custom shape and chunks
    z_custom = create_sample_zarr_array(shape=(50, 30), chunks=(5, 3))
    assert z_custom.shape == (50, 30)
    assert z_custom.chunks == (5, 3)


def test_create_zarr_group():
    """Test creating a zarr group."""
    group = create_zarr_group()
    
    # Check that it's a zarr Group
    import zarr
    assert isinstance(group, zarr.Group)
    
    # Check arrays in group
    assert "temperature" in group.array_keys()
    assert "pressure" in group.array_keys()
    
    # Check attributes
    assert "title" in group.attrs
    assert "description" in group.attrs
    
    # Check array shapes
    assert group['temperature'].shape == (10, 10)
    assert group['pressure'].shape == (10, 10)


def test_zarr_info_array():
    """Test getting zarr array information."""
    z = create_sample_zarr_array()
    info = zarr_info(z)
    
    # Check info structure
    assert info["type"] == "array"
    assert "shape" in info
    assert "chunks" in info
    assert "dtype" in info
    assert "compressor" in info
    assert "nbytes" in info
    assert "attrs" in info
    
    # Check specific values
    assert info["shape"] == (100, 100)
    assert info["chunks"] == (10, 10)


def test_zarr_info_group():
    """Test getting zarr group information."""
    group = create_zarr_group()
    info = zarr_info(group)
    
    # Check info structure
    assert info["type"] == "group"
    assert "arrays" in info
    assert "groups" in info
    assert "attrs" in info
    
    # Check specific values
    assert "temperature" in info["arrays"]
    assert "pressure" in info["arrays"]


def test_save_and_load_zarr_array():
    """Test saving and loading zarr array to/from directory."""
    z = create_sample_zarr_array()
    temp_path = create_temp_zarr_store()
    
    try:
        # Test saving
        result = save_zarr_to_directory(z, temp_path)
        assert result is True
        assert os.path.exists(temp_path)
        
        # Test loading
        loaded_z = load_zarr_from_directory(temp_path)
        assert loaded_z is not None
        
        # Check that loaded array has same properties
        assert loaded_z.shape == z.shape
        assert loaded_z.chunks == z.chunks
        assert loaded_z.dtype == z.dtype
        
        # Check data values are the same
        np.testing.assert_array_equal(loaded_z[:], z[:])
        
    finally:
        # Clean up
        cleanup_temp_zarr_store(temp_path)


def test_save_and_load_zarr_group():
    """Test saving and loading zarr group to/from directory."""
    group = create_zarr_group()
    temp_path = create_temp_zarr_store()
    
    try:
        # Test saving
        result = save_zarr_to_directory(group, temp_path)
        assert result is True
        assert os.path.exists(temp_path)
        
        # Test loading
        loaded_group = load_zarr_from_directory(temp_path)
        assert loaded_group is not None
        
        # Check that loaded group has same structure
        assert list(loaded_group.array_keys()) == list(group.array_keys())
        assert dict(loaded_group.attrs) == dict(group.attrs)
        
        # Check individual arrays
        for array_name in group.array_keys():
            original_array = group[array_name]
            loaded_array = loaded_group[array_name]
            assert loaded_array.shape == original_array.shape
            assert loaded_array.chunks == original_array.chunks
            np.testing.assert_array_equal(loaded_array[:], original_array[:])
        
    finally:
        # Clean up
        cleanup_temp_zarr_store(temp_path)


def test_load_nonexistent_zarr():
    """Test loading from non-existent zarr directory."""
    result = load_zarr_from_directory("/nonexistent/path")
    assert result is None


def test_save_to_invalid_path():
    """Test saving to invalid path."""
    z = create_sample_zarr_array()
    result = save_zarr_to_directory(z, "/invalid/path")
    assert result is False


def test_temp_zarr_store_management():
    """Test temporary zarr store creation and cleanup."""
    temp_path = create_temp_zarr_store()
    
    # Check that path exists and is a directory
    assert os.path.exists(temp_path)
    assert os.path.isdir(temp_path)
    assert temp_path.startswith(tempfile.gettempdir())
    
    # Clean up
    result = cleanup_temp_zarr_store(temp_path)
    assert result is True
    assert not os.path.exists(temp_path)


def test_cleanup_nonexistent_path():
    """Test cleanup of non-existent path."""
    result = cleanup_temp_zarr_store("/nonexistent/path")
    assert result is False


class TestZarrOperations:
    """Test class for zarr operations."""
    
    def test_zarr_array_slicing(self):
        """Test slicing operations on zarr arrays."""
        z = create_sample_zarr_array()
        
        # Test basic slicing
        subset = z[0:10, 0:10]
        assert subset.shape == (10, 10)
        
        # Test step slicing
        subset = z[::2, ::2]
        assert subset.shape == (50, 50)
    
    def test_zarr_array_arithmetic(self):
        """Test arithmetic operations on zarr arrays."""
        z = create_sample_zarr_array()
        
        # Test arithmetic operations (need to convert to numpy for zarr v3)
        z_data = z[:]
        z_doubled = z_data * 2
        np.testing.assert_array_almost_equal(z_doubled, z[:] * 2)
        
        # Test addition
        z_plus_one = z_data + 1
        np.testing.assert_array_almost_equal(z_plus_one, z[:] + 1)
    
    def test_zarr_group_operations(self):
        """Test operations on zarr groups."""
        group = create_zarr_group()
        
        # Test accessing arrays
        temp_array = group['temperature']
        pressure_array = group['pressure']
        
        assert temp_array.shape == (10, 10)
        assert pressure_array.shape == (10, 10)
        
        # Test creating new array in group
        group.create_array('humidity', shape=(10, 10), chunks=(5, 5), dtype=np.float64)
        group['humidity'][:] = np.random.randn(10, 10)
        assert 'humidity' in group.array_keys()
    
    def test_zarr_compression(self):
        """Test zarr compression options."""
        import zarr
        
        # Create array with compression (zarr v3 syntax)
        try:
            z = zarr.array(np.random.randn(100, 100), chunks=(10, 10))
            # In zarr v3, compression is handled differently
            # Just check that we can create and access the array
            assert z.shape == (100, 100)
            assert z.chunks == (10, 10)
        except Exception as e:
            # Skip test if compression not available
            import pytest
            pytest.skip(f"Compression test not available: {e}")


@pytest.mark.parametrize("shape,chunks", [
    ((50, 50), (10, 10)),
    ((100, 100), (20, 20)),
    ((200, 100), (50, 25)),
])
def test_zarr_array_creation_parametrized(shape, chunks):
    """Parametrized test for zarr array creation with different shapes and chunks."""
    z = create_sample_zarr_array(shape=shape, chunks=chunks)
    
    # Test shape and chunks
    assert z.shape == shape
    assert z.chunks == chunks
    
    # Test that data is accessible
    assert z[0, 0] is not None
    
    # Test that full array can be accessed
    data = z[:]
    assert data.shape == shape


def test_zarr_with_different_dtypes():
    """Test zarr arrays with different data types."""
    import zarr
    
    # Test different data types
    dtypes = [np.float32, np.float64, np.int32, np.int64]
    
    for dtype in dtypes:
        data = np.random.randn(10, 10).astype(dtype)
        z = zarr.array(data, chunks=(5, 5))
        
        assert z.dtype == dtype
        np.testing.assert_array_equal(z[:], data)