import os
import fsspec
import xarray as xr
import icechunk
import icechunk.xarray as icx
import pytest

from actions_package.azure_storage import AzuriteStorageClient
from actions_package.mock_data_generator import generate_mock_data

from tests.helpers import get_test_data_path, setup_icechunk_repo, total_sent_bytes


@pytest.mark.azurite
@pytest.mark.external_service
def test_azurite_basic_operations():
    client = AzuriteStorageClient()
    assert client.create_container() is True

    blob_name = "healthcheck.txt"
    content = "hello from tests"

    assert client.upload_blob(blob_name, content) is True
    assert client.blob_exists(blob_name) is True
    assert client.download_blob(blob_name) == content
    assert client.delete_blob(blob_name) is True


@pytest.mark.azurite
@pytest.mark.external_service
def test_azure_fsspec():
    """Use fsspec to write a file to azure storage via Azurite."""
    CONN_STRING = (
        "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;"
        "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
        "BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
    )

    fs = fsspec.filesystem("filecache", target_protocol="az", target_options={"connection_string": CONN_STRING})

    fs.rm("test-container")
    fs.mkdir("test-container")  # ok

    assert len(fs.ls("test-container")) == 0

    with fs.open("test-container/foo", mode="wb") as file_handle:
        file_handle.write(b"foo")

    assert len(fs.ls("test-container")) == 1


@pytest.mark.azurite
@pytest.mark.external_service
def test_azure_icechunk():
    """Create an empty icechunk repository on Azurite."""
    repo = setup_icechunk_repo("my-container", "my-prefix")
    assert repo is not None


@pytest.mark.azurite
@pytest.mark.external_service
@pytest.mark.integration
def test_azure_icechunk_xarray_upload(tmp_path):
    """Upload a NetCDF file to Azurite via icechunk using xarray."""
    repo = setup_icechunk_repo("xarray-container", "xarray-prefix")

    session = repo.writable_session("main")
    ds = xr.open_dataset(get_test_data_path())
    start_bytes = total_sent_bytes()
    icx.to_icechunk(ds, session, mode="w")
    session.commit("initial upload")
    used = total_sent_bytes() - start_bytes

    ro = repo.readonly_session("main")
    result = xr.open_dataset(ro.store, engine="zarr")

    assert len(result["timestamp"]) == len(ds["timestamp"])
    assert len(result["high_res_timestamp"]) == len(ds["high_res_timestamp"])
    assert used > 0


@pytest.mark.azurite
@pytest.mark.external_service
@pytest.mark.integration
@pytest.mark.slow
def test_azure_icechunk_append(tmp_path):
    """Append extended data to an existing icechunk store."""

    start_bytes = total_sent_bytes()

    repo = setup_icechunk_repo("append-container", "append-prefix")

    base_session = repo.writable_session("main")
    ds_seed = xr.open_dataset(get_test_data_path())
    icx.to_icechunk(ds_seed, base_session, mode="w")
    base_session.commit("initial")

    extended_path = tmp_path / "extended.nc"
    ds_extended = generate_mock_data(
        seed_file=get_test_data_path(),
        output_file=extended_path,
        target_size_mb=10,
    )


    ts_slice = slice(len(ds_seed["timestamp"]), None)
    ds_ts = ds_extended.isel(timestamp=ts_slice)
    ds_ts = ds_ts[[v for v in ds_ts.data_vars if "timestamp" in ds_ts[v].dims]]
    ds_ts = ds_ts.drop_dims("high_res_timestamp", errors="ignore")
    ts_session = repo.writable_session("main")
    icx.to_icechunk(ds_ts, ts_session, append_dim="timestamp")
    ts_session.commit("append timestamp")

    hr_slice = slice(len(ds_seed["high_res_timestamp"]), None)
    ds_hr = ds_extended.isel(high_res_timestamp=hr_slice)
    ds_hr = ds_hr[[v for v in ds_hr.data_vars if "high_res_timestamp" in ds_hr[v].dims]]
    hr_session = repo.writable_session("main")
    icx.to_icechunk(ds_hr, hr_session, append_dim="high_res_timestamp")
    hr_session.commit("append highres")

    used = total_sent_bytes() - start_bytes

    # --- NEW: sanity-check traffic vs. payload size ------------------------
    file_size = extended_path.stat().st_size  # bytes on disk
    # At least the payload, but allow a tight overhead (×2) for protocol traffic
    assert file_size <= used <= file_size * 2
    # ----------------------------------------------------------------------

    ro = repo.readonly_session("main")
    result = xr.open_dataset(ro.store, engine="zarr")

    assert len(result["timestamp"]) == len(ds_extended["timestamp"])
    assert len(result["high_res_timestamp"]) == len(ds_extended["high_res_timestamp"])
    assert used > 0
