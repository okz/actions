"""
Icechunk utilities for the actions package.
"""

import icechunk
import zarr
import numpy as np
from typing import Optional, Dict, Any
import tempfile
import os


class IcechunkStorageClient:
    """Client for interacting with Icechunk storage."""
    
    def __init__(self, storage_config: Optional[Dict[str, Any]] = None):
        """
        Initialize the Icechunk storage client.
        
        Args:
            storage_config: Configuration for storage backend. If None, uses memory storage.
        """
        self.storage_config = storage_config or {}
        self.store = None
        
    def create_store(self, path: Optional[str] = None) -> bool:
        """
        Create an icechunk store.
        
        Args:
            path: Optional path for file-based storage
            
        Returns:
            True if successful, False otherwise.
        """
        try:
            if path:
                # Use file-based storage
                self.store = icechunk.IcechunkStore.create(path)
            else:
                # Use memory storage for testing
                self.store = icechunk.IcechunkStore.create_memory()
            return True
        except Exception as e:
            print(f"Error creating icechunk store: {e}")
            return False
    
    def open_store(self, path: str) -> bool:
        """
        Open an existing icechunk store.
        
        Args:
            path: Path to existing store
            
        Returns:
            True if successful, False otherwise.
        """
        try:
            self.store = icechunk.IcechunkStore.open(path)
            return True
        except Exception as e:
            print(f"Error opening icechunk store: {e}")
            return False
    
    def create_zarr_group(self) -> Optional[zarr.Group]:
        """
        Create a zarr group using the icechunk store.
        
        Returns:
            zarr Group if successful, None otherwise.
        """
        try:
            if self.store is None:
                if not self.create_store():
                    return None
            
            group = zarr.group(store=self.store)
            return group
        except Exception as e:
            print(f"Error creating zarr group: {e}")
            return None
    
    def save_array_to_store(self, array_name: str, data: np.ndarray, chunks: tuple = None) -> bool:
        """
        Save numpy array to icechunk store.
        
        Args:
            array_name: Name of the array in the store
            data: Numpy array to save
            chunks: Chunk size for the array
            
        Returns:
            True if successful, False otherwise.
        """
        try:
            if self.store is None:
                if not self.create_store():
                    return False
            
            group = zarr.group(store=self.store)
            chunks = chunks or (min(100, data.shape[0]), min(100, data.shape[1]) if data.ndim > 1 else None)
            group.array(array_name, data, chunks=chunks)
            return True
        except Exception as e:
            print(f"Error saving array to store: {e}")
            return False
    
    def load_array_from_store(self, array_name: str) -> Optional[zarr.Array]:
        """
        Load array from icechunk store.
        
        Args:
            array_name: Name of the array in the store
            
        Returns:
            zarr Array if successful, None otherwise.
        """
        try:
            if self.store is None:
                return None
            
            group = zarr.group(store=self.store)
            return group[array_name]
        except Exception as e:
            print(f"Error loading array from store: {e}")
            return None
    
    def list_arrays(self) -> list:
        """
        List all arrays in the store.
        
        Returns:
            List of array names.
        """
        try:
            if self.store is None:
                return []
            
            group = zarr.group(store=self.store)
            return list(group.array_keys())
        except Exception as e:
            print(f"Error listing arrays: {e}")
            return []
    
    def commit_changes(self, message: str = "Commit changes") -> bool:
        """
        Commit changes to the store.
        
        Args:
            message: Commit message
            
        Returns:
            True if successful, False otherwise.
        """
        try:
            if self.store is None:
                return False
            
            # Commit changes if the store supports it
            if hasattr(self.store, 'commit'):
                self.store.commit(message)
            return True
        except Exception as e:
            print(f"Error committing changes: {e}")
            return False
    
    def get_store_info(self) -> Dict[str, Any]:
        """
        Get information about the store.
        
        Returns:
            Dictionary containing store information.
        """
        try:
            if self.store is None:
                return {"status": "no store"}
            
            info = {
                "status": "active",
                "type": type(self.store).__name__,
            }
            
            # Try to get additional info
            try:
                group = zarr.group(store=self.store)
                info["arrays"] = list(group.array_keys())
                info["groups"] = list(group.group_keys())
            except:
                pass
            
            return info
        except Exception as e:
            print(f"Error getting store info: {e}")
            return {"status": "error", "error": str(e)}


def create_sample_icechunk_data() -> np.ndarray:
    """
    Create sample data for icechunk testing.
    
    Returns:
        Numpy array with sample data.
    """
    np.random.seed(42)
    return np.random.randn(50, 50)


def create_temp_icechunk_path() -> str:
    """
    Create a temporary path for icechunk storage.
    
    Returns:
        Path to temporary directory.
    """
    return tempfile.mkdtemp(prefix="icechunk_test_")


def cleanup_temp_icechunk_path(path: str) -> bool:
    """
    Clean up temporary icechunk path.
    
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
        print(f"Error cleaning up temp icechunk path: {e}")
        return False