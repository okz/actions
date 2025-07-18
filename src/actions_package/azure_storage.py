"""
Azure Storage operations using Azurite emulator.
"""

import os
from typing import Optional
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient


class AzuriteStorageClient:
    """Client for interacting with Azure Blob Storage via Azurite emulator."""
    
    def __init__(self, connection_string: Optional[str] = None):
        """
        Initialize the Azurite storage client.
        
        Args:
            connection_string: Azure Storage connection string. If not provided,
                             uses the default Azurite connection string.
        """
        if connection_string is None:
            # Default Azurite connection string
            connection_string = (
                "DefaultEndpointsProtocol=http;"
                "AccountName=devstoreaccount1;"
                "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
                "BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
            )
        
        # Create the client with a specific API version that's compatible with Azurite
        try:
            self.blob_service_client = BlobServiceClient.from_connection_string(
                connection_string, 
                api_version='2020-10-02'  # Use an older API version compatible with Azurite
            )
        except Exception:
            # Fallback to default API version if the specific one fails
            self.blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        
        self.container_name = "test-container"
    
    def create_container(self) -> bool:
        """
        Create a container if it doesn't exist.
        
        Returns:
            True if container was created or already exists, False otherwise.
        """
        try:
            container_client = self.blob_service_client.get_container_client(self.container_name)
            container_client.create_container()
            return True
        except Exception as e:
            # Container might already exist
            if "ContainerAlreadyExists" in str(e):
                return True
            print(f"Error creating container: {e}")
            return False
    
    def upload_blob(self, blob_name: str, data: str) -> bool:
        """
        Upload a blob to the container.
        
        Args:
            blob_name: Name of the blob
            data: String data to upload
            
        Returns:
            True if upload was successful, False otherwise.
        """
        try:
            blob_client = self.blob_service_client.get_blob_client(
                container=self.container_name, 
                blob=blob_name
            )
            blob_client.upload_blob(data, overwrite=True)
            return True
        except Exception as e:
            print(f"Error uploading blob: {e}")
            return False
    
    def download_blob(self, blob_name: str) -> Optional[str]:
        """
        Download a blob from the container.
        
        Args:
            blob_name: Name of the blob to download
            
        Returns:
            String content of the blob if successful, None otherwise.
        """
        try:
            blob_client = self.blob_service_client.get_blob_client(
                container=self.container_name, 
                blob=blob_name
            )
            download_stream = blob_client.download_blob()
            return download_stream.readall().decode('utf-8')
        except Exception as e:
            print(f"Error downloading blob: {e}")
            return None
    
    def list_blobs(self) -> list[str]:
        """
        List all blobs in the container.
        
        Returns:
            List of blob names.
        """
        try:
            container_client = self.blob_service_client.get_container_client(self.container_name)
            blobs = container_client.list_blobs()
            return [blob.name for blob in blobs]
        except Exception as e:
            print(f"Error listing blobs: {e}")
            return []
    
    def delete_blob(self, blob_name: str) -> bool:
        """
        Delete a blob from the container.
        
        Args:
            blob_name: Name of the blob to delete
            
        Returns:
            True if deletion was successful, False otherwise.
        """
        try:
            blob_client = self.blob_service_client.get_blob_client(
                container=self.container_name, 
                blob=blob_name
            )
            blob_client.delete_blob()
            return True
        except Exception as e:
            print(f"Error deleting blob: {e}")
            return False
    
    def blob_exists(self, blob_name: str) -> bool:
        """
        Check if a blob exists in the container.
        
        Args:
            blob_name: Name of the blob to check
            
        Returns:
            True if blob exists, False otherwise.
        """
        try:
            blob_client = self.blob_service_client.get_blob_client(
                container=self.container_name, 
                blob=blob_name
            )
            return blob_client.exists()
        except Exception as e:
            print(f"Error checking blob existence: {e}")
            return False