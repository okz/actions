import os
from datetime import datetime, timedelta

import icechunk
import icechunk.xarray as icx
import xarray as xr

from tests.helpers import AzuriteStorageClient, open_test_dataset, find_latest_backup_repo


def _create_backup_repo(container_name: str, prefix: str, dataset: xr.Dataset) -> None:
    """Create a new icechunk repository in *container_name* under *prefix*."""
    client = AzuriteStorageClient()
    client.container_name = container_name
    client.create_container()

    storage = icechunk.azure_storage(
        account=os.environ["AZURE_STORAGE_ACCOUNT_NAME"],
        container=container_name,
        prefix=prefix,
        from_env=True,
        config={
            "azure_storage_use_emulator": "true",
            "azure_allow_http": "true",
        },
    )
    repo = icechunk.Repository.create(storage)
    session = repo.writable_session("main")
    icx.to_icechunk(dataset, session, mode="w")
    session.commit("initial upload")


def test_find_latest_backup_repo():
    """Ensure the latest backup repository can be located and opened."""
    container = "streaming-backup"
    client = AzuriteStorageClient()
    client.container_name = container
    try:
        client.blob_service_client.delete_container(container)
    except Exception:
        pass
    ds = open_test_dataset()
    instrument = ds.attrs["instrument"]
    project = ds.attrs["project"]
    prefixes = []

    base = datetime.utcnow()
    for i in range(3):
        ts = (base + timedelta(seconds=i)).strftime("%Y-%m-%dt%H-%M-%Sz")
        prefix = f"{instrument}/{project}/inst-{instrument}-prj-{project}-{ts}l1b"
        _create_backup_repo(container, prefix, ds)
        prefixes.append(prefix)

    repos = find_latest_backup_repo(f"az://{container}")
    assert repos == sorted(prefixes)
    latest = repos[-1]

    storage = icechunk.azure_storage(
        account=os.environ["AZURE_STORAGE_ACCOUNT_NAME"],
        container=container,
        prefix=latest,
        from_env=True,
        config={
            "azure_storage_use_emulator": "true",
            "azure_allow_http": "true",
        },
    )
    repo = icechunk.Repository.open(storage)
    ro = repo.readonly_session("main")
    loaded = xr.open_dataset(ro.store, engine="zarr")

    assert len(loaded["timestamp"]) == len(ds["timestamp"])
