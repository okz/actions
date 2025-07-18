"""
Zarr utilities for the actions package.
"""

import zarr
import numpy as np
from typing import Optional, Dict, Any, Union
import tempfile
import os


def create_sample_zarr_array(shape: tuple = (100, 100), chunks: tuple = (10, 10)) -> zarr.Array:
    """
    Create a sample zarr array for testing.
    
    Args:
        shape: Shape of the array
        chunks: Chunk size
        
    Returns:
        zarr Array with sample data.
    """
    # Create sample data
    np.random.seed(42)
    data = np.random.randn(*shape)
    
    # Create zarr array
    z = zarr.array(data, chunks=chunks)
    
    return z


def create_zarr_group() -> zarr.Group:
    """
    Create a sample zarr group for testing.
    
    Returns:
        zarr Group with sample data.
    """
    # Create root group
    root = zarr.group()
    
    # Add some arrays (zarr v3 syntax)
    root.create_array('temperature', shape=(10, 10), chunks=(5, 5), dtype=np.float64)
    root.create_array('pressure', shape=(10, 10), chunks=(5, 5), dtype=np.float64)
    
    # Fill with sample data
    root['temperature'][:] = np.random.randn(10, 10)
    root['pressure'][:] = np.random.randn(10, 10)
    
    # Add attributes
    root.attrs['title'] = 'Sample zarr group'
    root.attrs['description'] = 'Generated sample data for testing'
    
    return root


def save_zarr_to_directory(zarr_obj: Union[zarr.Array, zarr.Group], path: str) -> bool:
    """
    Save zarr array or group to directory.
    
    Args:
        zarr_obj: zarr Array or Group to save
        path: Output directory path
        
    Returns:
        True if successful, False otherwise.
    """
    try:
        # For zarr v3, we need to use the store directly
        if isinstance(zarr_obj, zarr.Array):
            # Save single array
            zarr_obj.store.sync()
            zarr_obj.store.export(path)
        else:
            # Save group
            zarr_obj.store.sync()
            zarr_obj.store.export(path)
        return True
    except Exception as e:
        print(f"Error saving zarr to directory: {e}")
        # Fallback: try to copy data manually
        try:
            import shutil
            import os
            if os.path.exists(path):
                shutil.rmtree(path)
            os.makedirs(path, exist_ok=True)
            
            # Simple implementation - just create a new zarr at the path
            if isinstance(zarr_obj, zarr.Array):
                new_array = zarr.open(path, mode='w', shape=zarr_obj.shape, chunks=zarr_obj.chunks, dtype=zarr_obj.dtype)
                new_array[:] = zarr_obj[:]
                new_array.attrs.update(zarr_obj.attrs)
            else:
                # For group, create new group and copy arrays
                new_group = zarr.group(path)
                for key in zarr_obj.array_keys():
                    arr = zarr_obj[key]
                    new_group.create_array(key, shape=arr.shape, chunks=arr.chunks, dtype=arr.dtype)
                    new_group[key][:] = arr[:]
                    new_group[key].attrs.update(arr.attrs)
                new_group.attrs.update(zarr_obj.attrs)
            return True
        except Exception as e2:
            print(f"Fallback also failed: {e2}")
            return False


def load_zarr_from_directory(path: str) -> Optional[Union[zarr.Array, zarr.Group]]:
    """
    Load zarr array or group from directory.
    
    Args:
        path: Input directory path
        
    Returns:
        zarr Array or Group if successful, None otherwise.
    """
    try:
        if os.path.exists(path):
            return zarr.open(path, mode='r')
        return None
    except Exception as e:
        print(f"Error loading zarr from directory: {e}")
        return None


def zarr_info(zarr_obj: Union[zarr.Array, zarr.Group]) -> Dict[str, Any]:
    """
    Get basic information about a zarr array or group.
    
    Args:
        zarr_obj: zarr Array or Group to analyze
        
    Returns:
        Dictionary containing zarr information.
    """
    if isinstance(zarr_obj, zarr.Array):
        info = {
            "type": "array",
            "shape": zarr_obj.shape,
            "chunks": zarr_obj.chunks,
            "dtype": str(zarr_obj.dtype),
            "nbytes": zarr_obj.nbytes,
            "attrs": dict(zarr_obj.attrs),
        }
        # Try to get compressor info (available in v2, not in v3)
        try:
            info["compressor"] = str(zarr_obj.compressor)
        except (AttributeError, TypeError):
            info["compressor"] = "not available"
        return info
    else:  # zarr.Group
        return {
            "type": "group",
            "arrays": list(zarr_obj.array_keys()),
            "groups": list(zarr_obj.group_keys()),
            "attrs": dict(zarr_obj.attrs),
        }


def create_temp_zarr_store() -> str:
    """
    Create a temporary directory for zarr storage.
    
    Returns:
        Path to temporary directory.
    """
    return tempfile.mkdtemp(prefix="zarr_test_")


def cleanup_temp_zarr_store(path: str) -> bool:
    """
    Clean up temporary zarr store.
    
    Args:
        path: Path to temporary directory
        
    Returns:
        True if successful, False otherwise.
    """
    try:
        import shutil
        shutil.rmtree(path)
        return True
    except Exception as e:
        print(f"Error cleaning up temp zarr store: {e}")
        return False