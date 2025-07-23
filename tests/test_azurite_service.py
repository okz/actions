import os
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


def test_azure_fsspec():
    """ Use fsspec to write a file to azure storage, emulated by Azurite"""
    CONN_STRING = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"

    fs = fsspec.filesystem("filecache", target_protocol="az",
        target_options={'connection_string': CONN_STRING})

    fs.rm("test-container")
    fs.mkdir("test-container") # ok

    assert len(fs.ls("test-container")) == 0

    with fs.open("test-container/foo", mode="wb") as file_handle:
        file_handle.write(b"foo") # ok

    assert len(fs.ls("test-container")) == 1


def test_azure_icechunk():
    """ Use icechunk to write a file to azure storage, emulated by Azurite"""

    import icechunk
    client = AzuriteStorageClient()
    client.container_name = "my-container"
    try:
        client.blob_service_client.delete_container(client.container_name)
    except Exception:
        pass
    client.create_container()

    storage = icechunk.azure_storage(
        account=os.environ["AZURE_STORAGE_ACCOUNT_NAME"],
        container="my-container",
        prefix="my-prefix",
        from_env=True,
        config={
            "azure_storage_use_emulator": "true",
            "azure_allow_http": "true",
        },
    )
    repo = icechunk.Repository.create(storage)
    
    # Verify repository was created successfully
    assert repo is not None

