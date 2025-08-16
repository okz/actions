import os
import numpy as np
import xarray as xr
import icechunk

from ice_stream.blocks import select_minimal_variables, upload_single_chunk, clean_dataset
from tests.helpers import AzuriteStorageClient, open_test_dataset


def _extend_to_24h(ds: xr.Dataset) -> xr.Dataset:
    """Extend dataset to cover at least 24 hours along timestamp dimension."""
    t = ds["timestamp"].values
    span = (t[-1] - t[0]) + (t[1] - t[0])
    factor = int(np.ceil(np.timedelta64(24, "h") / span))
    time_vars = [v for v in ds.data_vars if ds[v].dims == ("timestamp",)]
    other_vars = [v for v in ds.data_vars if v not in time_vars]
    parts = []
    for i in range(factor):
        part = ds[time_vars].copy(deep=True)
        part = part.assign_coords(timestamp=t + i * span)
        parts.append(part)
    extended_time = xr.concat(parts, dim="timestamp")
    return clean_dataset(xr.merge([extended_time, ds[other_vars]]))


def test_minimal_day_upload(artifacts) -> None:
    ds = open_test_dataset()
    ds_min = select_minimal_variables(ds)
    ds_day = _extend_to_24h(ds_min)

    container = "minimal-day-container"
    prefix = "minimal-day-prefix"
    client = AzuriteStorageClient()
    client.container_name = container
    try:
        client.blob_service_client.delete_container(container)
    except Exception:
        pass
    client.create_container()

    storage = icechunk.azure_storage(
        account=os.environ["AZURE_STORAGE_ACCOUNT_NAME"],
        container=container,
        prefix=prefix,
        from_env=True,
        config={"azure_storage_use_emulator": "true", "azure_allow_http": "true"},
    )
    repo = icechunk.Repository.create(storage)
    upload_single_chunk(repo, ds_day)

    container_client = client.blob_service_client.get_container_client(container)
    total_bytes = sum(blob.size for blob in container_client.list_blobs(name_starts_with=prefix))
    artifacts.save_text("blob_size.txt", f"{total_bytes}\n")
    assert total_bytes > 0
