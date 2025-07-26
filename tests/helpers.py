# Helper functions for tests involving Azurite and icechunk
from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

import psutil

from actions_package.azure_storage import AzuriteStorageClient


def get_test_data_path() -> Path:
    """Return path to small test dataset."""
    return Path(__file__).resolve().parent / "data" / "small_data.nc"


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

def find_latest_backup_repo(container_name: str, backup_root: str = "backup") -> list[str]:
    """Return backup repository prefixes in creation order."""
    client = AzuriteStorageClient()
    client.container_name = container_name
    blob_names = client.list_blobs(name_starts_with=f"{backup_root}/")
    prefixes: set[str] = set()
    for name in blob_names:
        parts = name.split("/", 2)
        if len(parts) >= 2:
            prefixes.add(parts[1])
    if not prefixes:
        raise FileNotFoundError("No backup repositories found")
    timestamps = sorted(prefixes)
    return [f"{backup_root}/{ts}" for ts in timestamps]
