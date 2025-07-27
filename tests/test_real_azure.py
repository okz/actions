import os
import time
from datetime import datetime

import pytest
import xarray as xr
import numpy as np
import icechunk
import icechunk.xarray as icx
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

from actions_package.mock_data_generator import generate_mock_data
from tests.helpers import get_test_data_path

# Load environment variables from tests/.env if present
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"), override=False)

# Determine if real Azure credentials are provided
_REQUIRED_VARS = [
    "AZURE_STORAGE_ACCOUNT",
    "AZURE_STORAGE_AUTH_TYPE",
    "CLADS_BACKUP_UPLOAD_TARGET",
    "AZURE_STORAGE_SAS_TOKEN",
]
_RUN_REAL_AZURE_TESTS = any(os.getenv(v) for v in _REQUIRED_VARS)
pytestmark = pytest.mark.skipif(
    not _RUN_REAL_AZURE_TESTS,
    reason="Real Azure credentials not provided",
)


@pytest.fixture(autouse=True)
def _disable_azurite_env(azurite_env, monkeypatch: pytest.MonkeyPatch) -> None:
    """Remove Azurite environment variables set by the default fixture."""
    monkeypatch.delenv("AZURE_STORAGE_CONNECTION_STRING", raising=False)
    monkeypatch.delenv("AZURE_STORAGE_ACCOUNT_NAME", raising=False)
    monkeypatch.delenv("AZURE_STORAGE_ACCOUNT_KEY", raising=False)
    monkeypatch.delenv("AZURITE_BLOB_STORAGE_URL", raising=False)


def _create_blob_service_client() -> BlobServiceClient:
    conn_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    account = os.getenv("AZURE_STORAGE_ACCOUNT") or os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
    sas_token = os.getenv("AZURE_STORAGE_SAS_TOKEN")
    if conn_str:
        return BlobServiceClient.from_connection_string(conn_str)
    if account and sas_token:
        return BlobServiceClient(account_url=f"https://{account}.blob.core.windows.net", credential=sas_token)
    if account:
        return BlobServiceClient(account_url=f"https://{account}.blob.core.windows.net")
    raise RuntimeError("No usable Azure credentials found")


def _parse_target(target: str) -> tuple[str, str]:
    target = target.replace("az://", "")
    if target.startswith("https://"):
        target = target.split("/", 3)[-1]
    parts = target.split("/", 1)
    container = parts[0]
    prefix = parts[1] if len(parts) > 1 else ""
    return container, prefix


def _setup_repo(container: str, prefix: str) -> icechunk.Repository:
    client = _create_blob_service_client()
    container_client = client.get_container_client(container)
    try:
        container_client.delete_container()
    except Exception:
        pass
    try:
        container_client.create_container()
    except Exception:
        pass

    storage = icechunk.azure_storage(
        account=os.environ["AZURE_STORAGE_ACCOUNT"],
        container=container,
        prefix=prefix,
        from_env=True,
    )
    return icechunk.Repository.create(storage)


def test_real_azure_accessible() -> None:
    """Ensure Azure Blob Storage account can be reached."""
    client = _create_blob_service_client()
    info = client.get_account_information()
    assert "sku_name" in info


def test_real_azure_upload_roundtrip() -> None:
    """Upload and download a small blob on real Azure."""
    target = os.environ["CLADS_BACKUP_UPLOAD_TARGET"]
    container, base_prefix = _parse_target(target)
    timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    blob_path = f"{base_prefix}/{timestamp}/roundtrip.txt" if base_prefix else f"{timestamp}/roundtrip.txt"

    client = _create_blob_service_client()
    container_client = client.get_container_client(container)
    container_client.upload_blob(blob_path, b"hello", overwrite=True)
    data = container_client.download_blob(blob_path).readall().decode()
    assert data == "hello"


def test_large_repo_read_performance(tmp_path) -> None:
    """Upload a large repo and ensure last 100 timestamps read quickly."""
    target = os.environ["CLADS_BACKUP_UPLOAD_TARGET"]
    container, base_prefix = _parse_target(target)
    timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    prefix = f"{base_prefix}/{timestamp}" if base_prefix else timestamp

    large_path = tmp_path / "large.nc"
    generate_mock_data(
        seed_file=get_test_data_path(),
        output_file=large_path,
        target_duration_hours=24 * 7,
    )

    repo = _setup_repo(container, prefix)
    ds = xr.open_dataset(large_path)
    # identify variables by dimension type
    ts_vars = [v for v in ds.data_vars if "timestamp" in ds[v].dims]
    hr_vars = [v for v in ds.data_vars if "high_res_timestamp" in ds[v].dims]

    ts_start = ds["timestamp"].values[0]
    ts_end = ds["timestamp"].values[-1]
    interval = np.timedelta64(15, "m")

    created_ts = False
    created_hr = False
    current = ts_start
    while current < ts_end:
        next_t = current + interval

        ts_chunk = ds.sel(timestamp=slice(current, next_t))
        ts_chunk = ts_chunk[[v for v in ts_chunk.data_vars if "timestamp" in ts_chunk[v].dims]]
        ts_chunk = ts_chunk.drop_dims("high_res_timestamp", errors="ignore")
        if ts_chunk.sizes.get("timestamp", 0) > 0:
            ts_session = repo.writable_session("main")
            mode = "w" if not created_ts else "a"
            kw = {"append_dim": "timestamp"} if created_ts else {}
            icx.to_icechunk(ts_chunk, ts_session, mode=mode, **kw)
            ts_session.commit(f"ts chunk {current}")
            created_ts = True

        hr_chunk = ds.sel(high_res_timestamp=slice(current, next_t))
        hr_chunk = hr_chunk[[v for v in hr_chunk.data_vars if "high_res_timestamp" in hr_chunk[v].dims]]
        if hr_chunk.sizes.get("high_res_timestamp", 0) > 0:
            hr_session = repo.writable_session("main")
            mode = "a" if (created_ts or created_hr) else "w"
            kw = {"append_dim": "high_res_timestamp"} if created_hr else {}
            icx.to_icechunk(hr_chunk, hr_session, mode=mode, **kw)
            hr_session.commit(f"hr chunk {current}")
            created_hr = True

        current = next_t

    ro = repo.readonly_session("main")
    ds_remote = xr.open_dataset(ro.store, engine="zarr")
    start = time.time()
    last = ds_remote["timestamp"].isel(timestamp=slice(-100, None)).load()
    elapsed = time.time() - start
    assert len(last) == 100
    assert elapsed < 1.0

