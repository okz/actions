"""
Tests for Azure Storage operations using Azurite emulator.
"""

import pytest
from actions_package.azure_storage import AzuriteStorageClient
import socket


def is_azurite_available() -> bool:
    """Check if Azurite is running and reachable."""
    # Quickly probe the default Azurite blob port with a short timeout
    try:
        with socket.create_connection(("127.0.0.1", 10000), timeout=1):
            pass
    except OSError:
        return False

    try:
        client = AzuriteStorageClient()
        return client.create_container()
    except Exception:
        return False


@pytest.fixture
def azurite_client():
    """Create an AzuriteStorageClient for testing."""
    if not is_azurite_available():
        pytest.fail("Azurite is not available for testing", pytrace=False)
    
    client = AzuriteStorageClient()
    # Ensure container exists
    client.create_container()
    yield client
    # Cleanup - delete all test blobs
    try:
        blobs = client.list_blobs()
        for blob in blobs:
            client.delete_blob(blob)
    except:
        pass  # Ignore cleanup errors


class TestAzuriteStorageClient:
    """Test cases for AzuriteStorageClient CRUD operations."""
    
    def test_create_container(self, azurite_client):
        """Test container creation."""
        result = azurite_client.create_container()
        assert result is True
    
    def test_upload_blob(self, azurite_client):
        """Test uploading a blob."""
        blob_name = "test-blob.txt"
        test_data = "Hello, Azurite!"
        
        result = azurite_client.upload_blob(blob_name, test_data)
        assert result is True
        
        # Verify blob exists
        assert azurite_client.blob_exists(blob_name) is True
    
    def test_download_blob(self, azurite_client):
        """Test downloading a blob."""
        blob_name = "test-download.txt"
        test_data = "Test download content"
        
        # Upload first
        azurite_client.upload_blob(blob_name, test_data)
        
        # Download and verify
        downloaded_data = azurite_client.download_blob(blob_name)
        assert downloaded_data == test_data
    
    def test_list_blobs(self, azurite_client):
        """Test listing blobs in container."""
        blob_names = ["blob1.txt", "blob2.txt", "blob3.txt"]
        
        # Upload multiple blobs
        for blob_name in blob_names:
            azurite_client.upload_blob(blob_name, f"Content for {blob_name}")
        
        # List blobs
        listed_blobs = azurite_client.list_blobs()
        
        # Verify all blobs are listed
        for blob_name in blob_names:
            assert blob_name in listed_blobs
    
    def test_delete_blob(self, azurite_client):
        """Test deleting a blob."""
        blob_name = "test-delete.txt"
        test_data = "This will be deleted"
        
        # Upload first
        azurite_client.upload_blob(blob_name, test_data)
        assert azurite_client.blob_exists(blob_name) is True
        
        # Delete blob
        result = azurite_client.delete_blob(blob_name)
        assert result is True
        
        # Verify blob no longer exists
        assert azurite_client.blob_exists(blob_name) is False
    
    def test_blob_exists(self, azurite_client):
        """Test checking if a blob exists."""
        blob_name = "existence-test.txt"
        
        # Initially should not exist
        assert azurite_client.blob_exists(blob_name) is False
        
        # Upload blob
        azurite_client.upload_blob(blob_name, "Test content")
        
        # Now should exist
        assert azurite_client.blob_exists(blob_name) is True
    
    def test_upload_overwrite(self, azurite_client):
        """Test uploading with overwrite."""
        blob_name = "overwrite-test.txt"
        original_content = "Original content"
        new_content = "New content"
        
        # Upload original
        azurite_client.upload_blob(blob_name, original_content)
        downloaded = azurite_client.download_blob(blob_name)
        assert downloaded == original_content
        
        # Upload new content (should overwrite)
        azurite_client.upload_blob(blob_name, new_content)
        downloaded = azurite_client.download_blob(blob_name)
        assert downloaded == new_content
    
    def test_download_nonexistent_blob(self, azurite_client):
        """Test downloading a blob that doesn't exist."""
        result = azurite_client.download_blob("nonexistent-blob.txt")
        assert result is None
    
    def test_delete_nonexistent_blob(self, azurite_client):
        """Test deleting a blob that doesn't exist."""
        result = azurite_client.delete_blob("nonexistent-blob.txt")
        assert result is False


@pytest.mark.parametrize("blob_name,content", [
    ("test1.txt", "Content 1"),
    ("test2.json", '{"key": "value"}'),
    ("test3.md", "# Markdown Content"),
    ("test4.csv", "col1,col2\nval1,val2"),
])
def test_crud_operations_parametrized(azurite_client, blob_name, content):
    """Parametrized test for CRUD operations."""
    # Create (Upload)
    assert azurite_client.upload_blob(blob_name, content) is True
    
    # Read (Download)
    downloaded_content = azurite_client.download_blob(blob_name)
    assert downloaded_content == content
    
    # Update (Upload with overwrite)
    updated_content = f"Updated: {content}"
    assert azurite_client.upload_blob(blob_name, updated_content) is True
    
    # Verify update
    downloaded_updated = azurite_client.download_blob(blob_name)
    assert downloaded_updated == updated_content
    
    # Delete
    assert azurite_client.delete_blob(blob_name) is True
    
    # Verify deletion
    assert azurite_client.blob_exists(blob_name) is False


# Test that the module can be imported even without Azurite
def test_azurite_storage_client_import():
    """Test that AzuriteStorageClient can be imported."""
    from actions_package.azure_storage import AzuriteStorageClient
    assert AzuriteStorageClient is not None


def test_azurite_storage_client_instantiation():
    """Test that AzuriteStorageClient can be instantiated."""
    client = AzuriteStorageClient()
    assert client is not None
    assert client.container_name == "test-container"