import os
from pathlib import Path
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


def test_azure_icechunk_xarray_upload(tmp_path):
    """Upload a NetCDF file to Azurite via icechunk using xarray."""

    import icechunk
    import icechunk.xarray as icx
    import xarray as xr

    client = AzuriteStorageClient()
    client.container_name = "xarray-container"
    try:
        client.blob_service_client.delete_container(client.container_name)
    except Exception:
        pass
    client.create_container()

    storage = icechunk.azure_storage(
        account=os.environ["AZURE_STORAGE_ACCOUNT_NAME"],
        container=client.container_name,
        prefix="xarray-prefix",
        from_env=True,
        config={
            "azure_storage_use_emulator": "true",
            "azure_allow_http": "true",
        },
    )
    repo = icechunk.Repository.create(storage)

    session = repo.writable_session("main")
    ds = xr.open_dataset(Path("tests/data/small_data.nc"))
    icx.to_icechunk(ds, session, mode="w")
    session.commit("initial upload")

    ro = repo.readonly_session("main")
    result = xr.open_dataset(ro.store, engine="zarr")

    assert len(result["timestamp"]) == len(ds["timestamp"])
    assert len(result["high_res_timestamp"]) == len(ds["high_res_timestamp"])


def test_azure_icechunk_append(tmp_path):
    """Append extended data to an existing icechunk store."""

    import icechunk
    import icechunk.xarray as icx
    import xarray as xr
    from actions_package.mock_data_generator import generate_mock_data

    client = AzuriteStorageClient()
    client.container_name = "append-container"
    try:
        client.blob_service_client.delete_container(client.container_name)
    except Exception:
        pass
    client.create_container()

    storage = icechunk.azure_storage(
        account=os.environ["AZURE_STORAGE_ACCOUNT_NAME"],
        container=client.container_name,
        prefix="append-prefix",
        from_env=True,
        config={
            "azure_storage_use_emulator": "true",
            "azure_allow_http": "true",
        },
    )
    repo = icechunk.Repository.create(storage)

    # Initial upload
    base_session = repo.writable_session("main")
    ds_seed = xr.open_dataset(Path("tests/data/small_data.nc"))
    icx.to_icechunk(ds_seed, base_session, mode="w")
    base_session.commit("initial")

    # Generate extended dataset
    extended_path = tmp_path / "extended.nc"
    ds_extended = generate_mock_data(
        seed_file=Path("tests/data/small_data.nc"),
        output_file=extended_path,
        target_size_mb=10,
    )

    # Append new timestamp data
    ts_slice = slice(len(ds_seed["timestamp"]), None)
    ds_ts = ds_extended.isel(timestamp=ts_slice)
    ds_ts = ds_ts[[v for v in ds_ts.data_vars if "timestamp" in ds_ts[v].dims]]
    ds_ts = ds_ts.drop_dims("high_res_timestamp", errors="ignore")
    ts_session = repo.writable_session("main")
    icx.to_icechunk(ds_ts, ts_session, append_dim="timestamp")
    ts_session.commit("append timestamp")

    # Append new high resolution timestamp data
    hr_slice = slice(len(ds_seed["high_res_timestamp"]), None)
    ds_hr = ds_extended.isel(high_res_timestamp=hr_slice)
    ds_hr = ds_hr[[v for v in ds_hr.data_vars if "high_res_timestamp" in ds_hr[v].dims]]
    hr_session = repo.writable_session("main")
    icx.to_icechunk(ds_hr, hr_session, append_dim="high_res_timestamp")
    hr_session.commit("append highres")

    ro = repo.readonly_session("main")
    result = xr.open_dataset(ro.store, engine="zarr")

    assert len(result["timestamp"]) == len(ds_extended["timestamp"])
    assert len(result["high_res_timestamp"]) == len(ds_extended["high_res_timestamp"])


