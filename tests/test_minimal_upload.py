import os
import numpy as np
import xarray as xr
import icechunk
import icechunk.xarray as icx  # added

from ice_stream.blocks import select_minimal_variables, upload_single_chunk, clean_dataset
from tests.helpers import AzuriteStorageClient, open_test_dataset, total_sent_bytes  # added


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
    
    start_sent = total_sent_bytes()

    storage = icechunk.azure_storage(
        account=os.environ["AZURE_STORAGE_ACCOUNT_NAME"],
        container=container,
        prefix=prefix,
        from_env=True,
        config={"azure_storage_use_emulator": "true", "azure_allow_http": "true"},
    )
    repo = icechunk.Repository.create(storage)

    # measure bytes sent
    upload_single_chunk(repo, ds_day)
    used_sent = max(0, total_sent_bytes() - start_sent)

    container_client = client.blob_service_client.get_container_client(container)
    total_bytes = sum(blob.size for blob in container_client.list_blobs(name_starts_with=prefix))
    size_mb = total_bytes / (1024 * 1024)
    num_timestamps = int(ds_day.sizes.get("timestamp", 0))
    var_names = list(ds_day.data_vars)
    dims_by_var = {v: {d: int(ds_day[v].sizes[d]) for d in ds_day[v].dims} for v in var_names}
    info_lines = [
        f"total_bytes: {total_bytes}",
        f"size_mb: {size_mb:.2f}",
        f"sent_bytes: {used_sent}",
        f"sent_mb: {used_sent/(1024*1024):.2f}",
        f"num_timestamps: {num_timestamps}",
        f"variables: {', '.join(var_names)}",
        "dims_by_var:",
    ]
    for v in var_names:
        dims_str = ", ".join(f"{d}={dims_by_var[v][d]}" for d in ds_day[v].dims)
        info_lines.append(f"  - {v}: {dims_str}")
    artifacts.save_text("blob_size.txt", "\n".join(info_lines) + "\n")
    assert total_bytes > 0


def test_minimal_day_upload_incremental(artifacts) -> None:
    """Upload minimal variables for ~24h in 15-minute chunks, reopening the repo for each append."""
    ds = open_test_dataset()
    ds_min = select_minimal_variables(ds)
    ds_day = _extend_to_24h(ds_min)
    ds_day = clean_dataset(ds_day)

    container = "minimal-day-incremental-container"
    prefix = "minimal-day-incremental-prefix"
    client = AzuriteStorageClient()
    client.container_name = container
    try:
        client.blob_service_client.delete_container(container)
    except Exception:
        pass
    client.create_container()

    # measure bytes sent across the whole incremental upload
    start_sent = total_sent_bytes()

    storage = icechunk.azure_storage(
        account=os.environ["AZURE_STORAGE_ACCOUNT_NAME"],
        container=container,
        prefix=prefix,
        from_env=True,
        config={"azure_storage_use_emulator": "true", "azure_allow_http": "true"},
    )

    t0 = ds_day["timestamp"].values[0]
    tN = ds_day["timestamp"].values[-1]
    interval = np.timedelta64(15, "m")
    first_end = t0 + interval


    # First chunk: create repo (mode="w")
    first_chunk = ds_day.sel(timestamp=slice(t0, first_end))
    repo = icechunk.Repository.create(storage)
    s = repo.writable_session("main")
    icx.to_icechunk(first_chunk, s, mode="w")
    s.commit("initial 15m chunk")

    # Subsequent chunks: reopen repo and append each time
    current = first_end
    while current < tN:
        next_t = current + interval
        chunk = ds_day.sel(timestamp=slice(current, next_t))
        if chunk.sizes.get("timestamp", 0) > 0:
            reopened = icechunk.Repository.open(storage)
            s2 = reopened.writable_session("main")
            icx.to_icechunk(chunk, s2, mode="a", append_dim="timestamp")
            s2.commit("append 15m chunk")
        current = next_t

    used_sent = max(0, total_sent_bytes() - start_sent)

    container_client = client.blob_service_client.get_container_client(container)
    total_bytes = sum(blob.size for blob in container_client.list_blobs(name_starts_with=prefix))
    size_mb = total_bytes / (1024 * 1024)
    num_timestamps = int(ds_day.sizes.get("timestamp", 0))
    var_names = list(ds_day.data_vars)
    dims_by_var = {v: {d: int(ds_day[v].sizes[d]) for d in ds_day[v].dims} for v in var_names}
    lines = [
        f"total_bytes: {total_bytes}",
        f"size_mb: {size_mb:.2f}",
        f"sent_bytes: {used_sent}",
        f"sent_mb: {used_sent/(1024*1024):.2f}",
        f"num_timestamps: {num_timestamps}",
        f"variables: {', '.join(var_names)}",
        "dims_by_var:",
    ]
    for v in var_names:
        dims_str = ", ".join(f"{d}={dims_by_var[v][d]}" for d in ds_day[v].dims)
        lines.append(f"  - {v}: {dims_str}")
    artifacts.save_text("blob_size_incremental.txt", "\n".join(lines) + "\n")

    assert total_bytes > 0


