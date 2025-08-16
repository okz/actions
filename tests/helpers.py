# Helper functions for tests involving Azurite and icechunk
from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Optional

import psutil

from actions_package.azure_storage import AzuriteStorageClient
from zarr.storage import ZipStore
import xarray as xr


def get_test_data_path() -> Path:
    """Return path to small test dataset."""
    return Path(__file__).resolve().parent / "data" / "small_data.zarr.zip"


def open_test_dataset() -> xr.Dataset:
    """Open the small test dataset from the zipped zarr file."""
    path = get_test_data_path()
    with ZipStore(path, mode="r") as store:
        ds = xr.open_zarr(store)
        ds.load()
    for var in ds.variables:
        ds[var].encoding.clear()
    return ds


def setup_icechunk_repo(container_name: str, prefix: str):
    """Create an icechunk repository in a new Azurite container."""
    import icechunk

    client = AzuriteStorageClient()
    client.container_name = container_name
    try:
        client.blob_service_client.delete_container(client.container_name)
    except Exception:
        pass
    client.create_container()

    storage = icechunk.azure_storage(
        account=os.environ["AZURE_STORAGE_ACCOUNT_NAME"],
        container=client.container_name,
        prefix=prefix,
        from_env=True,
        config={
            "azure_storage_use_emulator": "true",
            "azure_allow_http": "true",
        },
    )
    return icechunk.Repository.create(storage)


def total_sent_bytes(interface: Optional[str] = None) -> int:
    """Return total bytes sent on the given interface."""
    counters = psutil.net_io_counters(pernic=True)
    if interface:
        stat = counters.get(interface)
        if stat is None:
            return 0
        return stat.bytes_sent
    return sum(c.bytes_sent for c in counters.values())

def find_latest_backup_repo(target: Optional[str] = None) -> list[str]:
    """Return backup repository prefixes in chronological order.

    Parameters
    ----------
    target : str, optional
        Root location to search. Defaults to the
        ``CLADS_BACKUP_UPLOAD_TARGET`` environment variable.
    """

    target = target or os.environ["CLADS_BACKUP_UPLOAD_TARGET"]
    pattern = re.compile(
        r"inst-[^/]+-prj-[^/]+-\d{4}-\d{2}-\d{2}t\d{2}-\d{2}-\d{2}zl1b(min)?$"
    )

    if target.startswith("az://") or target.startswith("https://") or target.startswith("http://"):
        target = target.replace("az://", "")
        if target.startswith("https://") or target.startswith("http://"):
            target = target.split("/", 3)[-1]
        parts = target.split("/", 1)
        container = parts[0]
        base_prefix = parts[1] if len(parts) > 1 else ""

        client = AzuriteStorageClient()
        container_client = client.blob_service_client.get_container_client(container)
        repos: list[str] = []

        for inst in container_client.walk_blobs(name_starts_with=base_prefix, delimiter="/"):
            inst_prefix = inst.name
            for proj in container_client.walk_blobs(name_starts_with=inst_prefix, delimiter="/"):
                proj_prefix = proj.name
                for repo in container_client.walk_blobs(name_starts_with=proj_prefix, delimiter="/"):
                    full = repo.name.rstrip("/")
                    name = full.split("/")[-1]
                    if pattern.fullmatch(name):
                        if base_prefix:
                            full = full[len(base_prefix) + 1 :]
                        repos.append(full)

        if not repos:
            raise FileNotFoundError("No backup repositories found")
        repos.sort()
        return repos

    root = Path(target)
    repos: list[str] = []
    for inst in root.iterdir():
        if not inst.is_dir():
            continue
        for proj in inst.iterdir():
            if not proj.is_dir():
                continue
            for repo in proj.iterdir():
                if repo.is_dir() and pattern.fullmatch(repo.name):
                    repos.append(str(repo.relative_to(root)))

    if not repos:
        raise FileNotFoundError("No backup repositories found")
    repos.sort()
    return repos
