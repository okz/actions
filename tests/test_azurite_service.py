import numpy as np
import xarray as xr
import zarr
import fsspec
from actions_package.azure_storage import AzuriteStorageClient


def test_azurite_basic_operations():
    client = AzuriteStorageClient()
    assert client.create_container() is True

    blob_name = "healthcheck.txt"
    content = "hello from tests"

    assert client.upload_blob(blob_name, content) is True
    assert client.blob_exists(blob_name) is True
    assert client.download_blob(blob_name) == content
    assert client.delete_blob(blob_name) is True


def test_xarray_zarr_azure_fsspec():
    """Test xarray opening zarr datasets directly from Azure blob storage via fsspec."""
    import tempfile
    import os
    
    client = AzuriteStorageClient()
    
    # Ensure container exists
    assert client.create_container() is True
    
    # Create sample xarray dataset for testing
    time = np.arange("2024-01-01", "2024-01-06", dtype="datetime64[D]")
    lat = np.linspace(-10, 10, 21)
    lon = np.linspace(-10, 10, 21)
    
    # Create smaller test dataset for faster upload/download
    temperature = 15 + 5 * np.random.randn(len(time), len(lat), len(lon))
    
    ds = xr.Dataset(
        {
            "temperature": (["time", "lat", "lon"], temperature),
            "humidity": (["time", "lat", "lon"], 50 + 20 * np.random.randn(len(time), len(lat), len(lon))),
        },
        coords={"time": time, "lat": lat, "lon": lon},
    )
    
    # Add attributes
    ds.attrs["title"] = "Test Climate Data"
    ds["temperature"].attrs["units"] = "degrees_celsius"
    ds["humidity"].attrs["units"] = "percent"
    
    zarr_container_path = "test-climate-data.zarr"
    
    try:
        # Step 1: Create zarr dataset locally first, then upload to Azure
        with tempfile.TemporaryDirectory() as temp_dir:
            local_zarr_path = os.path.join(temp_dir, "climate_data.zarr")
            
            # Save to local zarr store
            ds.to_zarr(local_zarr_path, mode="w")
            
            # Upload zarr store to Azure blob storage
            # We need to upload each file in the zarr store
            for root, dirs, files in os.walk(local_zarr_path):
                for file in files:
                    local_file_path = os.path.join(root, file)
                    relative_path = os.path.relpath(local_file_path, local_zarr_path)
                    blob_name = f"{zarr_container_path}/{relative_path}".replace("\\", "/")
                    
                    with open(local_file_path, "rb") as f:
                        content = f.read()
                    
                    # Upload as binary data
                    success = client.upload_blob(blob_name, content, is_binary=True)
                    assert success, f"Failed to upload {blob_name}"
        
        # Step 2: Configure fsspec storage options for Azurite
        storage_options = {
            "account_name": "devstoreaccount1",  # Default Azurite account
            "account_key": "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==",  # Default Azurite key
            "connection_string": "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;",
        }
        
        # Step 3: Create Azure blob URL for the zarr dataset
        zarr_url = f"az://test-container/{zarr_container_path}"
        
        # Step 4: Test opening zarr dataset directly via fsspec and xarray
        try:
            ds_loaded = xr.open_dataset(
                zarr_url,
                engine="zarr",
                backend_kwargs=dict(storage_options=storage_options)
            )
            
            # Verify the dataset was loaded correctly
            assert "temperature" in ds_loaded.data_vars
            assert "humidity" in ds_loaded.data_vars
            assert ds_loaded.dims["time"] == len(time)
            assert ds_loaded.dims["lat"] == len(lat)
            assert ds_loaded.dims["lon"] == len(lon)
            
            # Verify attributes
            assert ds_loaded.attrs["title"] == "Test Climate Data"
            assert ds_loaded["temperature"].attrs["units"] == "degrees_celsius"
            
            # Test data access
            temp_data = ds_loaded["temperature"].values
            assert temp_data.shape == (len(time), len(lat), len(lon))
            
            # Test selective loading
            temp_subset = ds_loaded["temperature"].sel(time="2024-01-03")
            assert temp_subset.dims == {"lat": len(lat), "lon": len(lon)}
            
            print("Successfully opened zarr dataset from Azure blob storage via fsspec!")
            
        
    finally:
        # Cleanup: delete all uploaded zarr files
        try:
            # List and delete all blobs with the zarr prefix
            blobs = client.list_blobs()
            for blob_name in blobs:
                if blob_name.startswith(zarr_container_path):
                    client.delete_blob(blob_name)
        except Exception:
            pass  # Ignore cleanup errors
