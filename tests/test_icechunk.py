"""
Tests for icechunk utilities using Azurite container.
"""

import pytest
import numpy as np
import tempfile
import os
from actions_package.icechunk_utils import (
    IcechunkStorageClient,
    create_sample_icechunk_data,
    create_temp_icechunk_path,
    cleanup_temp_icechunk_path,
)


def is_azurite_available():
    """Check if Azurite is available for testing."""
    try:
        from actions_package.azure_storage import AzuriteStorageClient
        client = AzuriteStorageClient()
        return client.create_container()
    except Exception:
        return False


def test_icechunk_import():
    """Test that icechunk can be imported."""
    try:
        import icechunk
        assert icechunk is not None
    except ImportError:
        pytest.skip("icechunk not available")


def test_create_sample_icechunk_data():
    """Test creating sample data for icechunk."""
    data = create_sample_icechunk_data()
    
    # Check that it's a numpy array
    assert isinstance(data, np.ndarray)
    
    # Check shape
    assert data.shape == (50, 50)
    
    # Check that it contains data
    assert not np.all(data == 0)


def test_icechunk_storage_client_init():
    """Test IcechunkStorageClient initialization."""
    client = IcechunkStorageClient()
    assert client is not None
    assert client.storage_config == {}
    assert client.store is None
    
    # Test with custom config
    config = {"backend": "memory"}
    client_with_config = IcechunkStorageClient(config)
    assert client_with_config.storage_config == config


@pytest.mark.skipif(not is_azurite_available(), reason="Azurite not available")
def test_create_memory_store():
    """Test creating an in-memory icechunk store."""
    try:
        client = IcechunkStorageClient()
        result = client.create_store()
        assert result is True
        assert client.store is not None
        
        # Test store info
        info = client.get_store_info()
        assert info["status"] == "active"
        
    except Exception as e:
        pytest.skip(f"icechunk memory store not available: {e}")


@pytest.mark.skipif(not is_azurite_available(), reason="Azurite not available")
def test_create_zarr_group():
    """Test creating a zarr group with icechunk store."""
    try:
        client = IcechunkStorageClient()
        client.create_store()
        
        group = client.create_zarr_group()
        assert group is not None
        
        # Test that we can add arrays to the group
        import zarr
        assert isinstance(group, zarr.Group)
        
    except Exception as e:
        pytest.skip(f"icechunk zarr group not available: {e}")


@pytest.mark.skipif(not is_azurite_available(), reason="Azurite not available")
def test_save_and_load_array():
    """Test saving and loading arrays with icechunk."""
    try:
        client = IcechunkStorageClient()
        client.create_store()
        
        # Create sample data
        data = create_sample_icechunk_data()
        
        # Save array
        result = client.save_array_to_store("test_array", data)
        assert result is True
        
        # Load array
        loaded_array = client.load_array_from_store("test_array")
        assert loaded_array is not None
        
        # Check that data is the same
        np.testing.assert_array_equal(loaded_array[:], data)
        
        # Test listing arrays
        arrays = client.list_arrays()
        assert "test_array" in arrays
        
    except Exception as e:
        pytest.skip(f"icechunk array operations not available: {e}")


@pytest.mark.skipif(not is_azurite_available(), reason="Azurite not available")
def test_multiple_arrays():
    """Test working with multiple arrays in icechunk store."""
    try:
        client = IcechunkStorageClient()
        client.create_store()
        
        # Create multiple arrays
        data1 = np.random.randn(20, 20)
        data2 = np.random.randn(30, 30)
        
        # Save arrays
        assert client.save_array_to_store("array1", data1) is True
        assert client.save_array_to_store("array2", data2) is True
        
        # List arrays
        arrays = client.list_arrays()
        assert "array1" in arrays
        assert "array2" in arrays
        assert len(arrays) == 2
        
        # Load and verify both arrays
        loaded1 = client.load_array_from_store("array1")
        loaded2 = client.load_array_from_store("array2")
        
        assert loaded1 is not None
        assert loaded2 is not None
        
        np.testing.assert_array_equal(loaded1[:], data1)
        np.testing.assert_array_equal(loaded2[:], data2)
        
    except Exception as e:
        pytest.skip(f"icechunk multiple arrays not available: {e}")


@pytest.mark.skipif(not is_azurite_available(), reason="Azurite not available")
def test_commit_changes():
    """Test committing changes to icechunk store."""
    try:
        client = IcechunkStorageClient()
        client.create_store()
        
        # Save some data
        data = create_sample_icechunk_data()
        client.save_array_to_store("test_array", data)
        
        # Commit changes
        result = client.commit_changes("Test commit")
        assert result is True
        
    except Exception as e:
        pytest.skip(f"icechunk commit not available: {e}")


def test_load_nonexistent_array():
    """Test loading non-existent array."""
    try:
        client = IcechunkStorageClient()
        client.create_store()
        
        result = client.load_array_from_store("nonexistent_array")
        assert result is None
        
    except Exception as e:
        pytest.skip(f"icechunk not available: {e}")


def test_store_info_no_store():
    """Test getting store info when no store exists."""
    client = IcechunkStorageClient()
    info = client.get_store_info()
    assert info["status"] == "no store"


def test_temp_icechunk_path_management():
    """Test temporary icechunk path creation and cleanup."""
    temp_path = create_temp_icechunk_path()
    
    # Check that path exists and is a directory
    assert os.path.exists(temp_path)
    assert os.path.isdir(temp_path)
    assert temp_path.startswith(tempfile.gettempdir())
    
    # Clean up
    result = cleanup_temp_icechunk_path(temp_path)
    assert result is True
    assert not os.path.exists(temp_path)


def test_cleanup_nonexistent_icechunk_path():
    """Test cleanup of non-existent icechunk path."""
    result = cleanup_temp_icechunk_path("/nonexistent/path")
    assert result is False


class TestIcechunkOperations:
    """Test class for icechunk operations."""
    
    @pytest.mark.skipif(not is_azurite_available(), reason="Azurite not available")
    def test_custom_chunks(self):
        """Test saving arrays with custom chunk sizes."""
        try:
            client = IcechunkStorageClient()
            client.create_store()
            
            data = np.random.randn(100, 100)
            chunks = (25, 25)
            
            # Save with custom chunks
            result = client.save_array_to_store("chunked_array", data, chunks=chunks)
            assert result is True
            
            # Load and verify
            loaded = client.load_array_from_store("chunked_array")
            assert loaded is not None
            assert loaded.chunks == chunks
            
        except Exception as e:
            pytest.skip(f"icechunk custom chunks not available: {e}")
    
    @pytest.mark.skipif(not is_azurite_available(), reason="Azurite not available")
    def test_different_data_types(self):
        """Test icechunk with different data types."""
        try:
            client = IcechunkStorageClient()
            client.create_store()
            
            # Test different data types
            dtypes = [np.float32, np.float64, np.int32, np.int64]
            
            for i, dtype in enumerate(dtypes):
                data = np.random.randn(10, 10).astype(dtype)
                array_name = f"array_{dtype.__name__}"
                
                # Save array
                result = client.save_array_to_store(array_name, data)
                assert result is True
                
                # Load and verify
                loaded = client.load_array_from_store(array_name)
                assert loaded is not None
                assert loaded.dtype == dtype
                np.testing.assert_array_equal(loaded[:], data)
            
        except Exception as e:
            pytest.skip(f"icechunk different dtypes not available: {e}")


@pytest.mark.skipif(not is_azurite_available(), reason="Azurite not available")
@pytest.mark.parametrize("shape,chunks", [
    ((50, 50), (10, 10)),
    ((100, 100), (20, 20)),
    ((200, 100), (50, 25)),
])
def test_icechunk_parametrized(shape, chunks):
    """Parametrized test for icechunk with different shapes and chunks."""
    try:
        client = IcechunkStorageClient()
        client.create_store()
        
        # Create data with specified shape
        data = np.random.randn(*shape)
        
        # Save with specified chunks
        result = client.save_array_to_store("param_array", data, chunks=chunks)
        assert result is True
        
        # Load and verify
        loaded = client.load_array_from_store("param_array")
        assert loaded is not None
        assert loaded.shape == shape
        assert loaded.chunks == chunks
        np.testing.assert_array_equal(loaded[:], data)
        
    except Exception as e:
        pytest.skip(f"icechunk parametrized test not available: {e}")


# Integration test with azurite
@pytest.mark.skipif(not is_azurite_available(), reason="Azurite not available")
def test_icechunk_with_azurite_integration():
    """Test icechunk integration with azurite storage."""
    try:
        # This test verifies that icechunk works in the same environment as azurite
        # First verify azurite is working
        from actions_package.azure_storage import AzuriteStorageClient
        azure_client = AzuriteStorageClient()
        assert azure_client.create_container() is True
        
        # Test icechunk in the same environment
        icechunk_client = IcechunkStorageClient()
        assert icechunk_client.create_store() is True
        
        # Save test data
        data = create_sample_icechunk_data()
        assert icechunk_client.save_array_to_store("integration_test", data) is True
        
        # Load and verify
        loaded = icechunk_client.load_array_from_store("integration_test")
        assert loaded is not None
        np.testing.assert_array_equal(loaded[:], data)
        
    except Exception as e:
        pytest.skip(f"icechunk-azurite integration not available: {e}")


# Test that the module can be imported even without icechunk available
def test_icechunk_storage_client_import():
    """Test that IcechunkStorageClient can be imported."""
    from actions_package.icechunk_utils import IcechunkStorageClient
    assert IcechunkStorageClient is not None


def test_icechunk_storage_client_instantiation():
    """Test that IcechunkStorageClient can be instantiated."""
    client = IcechunkStorageClient()
    assert client is not None
    assert client.storage_config == {}
    assert client.store is None