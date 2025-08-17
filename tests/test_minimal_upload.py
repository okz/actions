import os
import datetime
import numpy as np
import pytest
import xarray as xr

import icechunk
import icechunk.xarray as icx

from ice_stream.blocks import clean_dataset, select_minimal_variables, upload_single_chunk
from icechunk import (
    ManifestSplitCondition,
    ManifestSplittingConfig,
    ManifestSplitDimCondition,
)
from tests.helpers import AzuriteStorageClient, open_test_dataset, total_sent_bytes


def _extend_to_hours(ds: xr.Dataset, hours: int = 1) -> xr.Dataset:
    """Extend dataset to cover *hours* along the timestamp dimension."""

    t = ds["timestamp"].values
    span = (t[-1] - t[0]) + (t[1] - t[0])
    target = np.timedelta64(hours, "h")
    factor = int(np.ceil(target / span))
    time_vars = [v for v in ds.data_vars if ds[v].dims == ("timestamp",)]
    other_vars = [v for v in ds.data_vars if v not in time_vars]
    parts: list[xr.Dataset] = []
    for i in range(factor):
        part = ds[time_vars].copy(deep=True)
        part = part.assign_coords(timestamp=t + i * span)
        parts.append(part)
    extended_time = xr.concat(parts, dim="timestamp")
    extended = clean_dataset(xr.merge([extended_time, ds[other_vars]]))
    end_time = extended["timestamp"].values[0] + target
    return extended.sel(timestamp=slice(None, end_time))


def _setup_repo(container: str, prefix: str, repo_config: icechunk.RepositoryConfig | None = None):
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
    repo = icechunk.Repository.create(storage, config=repo_config) if repo_config else icechunk.Repository.create(storage)
    return repo, client, storage


def _log_stats(
    ds: xr.Dataset,
    artifacts,
    client: AzuriteStorageClient,
    container: str,
    prefix: str,
    start_sent: int,
    repo: icechunk.Repository | None = None,
    name: str = "blob_size.txt",
) -> None:
    used_sent = max(0, total_sent_bytes() - start_sent)
    container_client = client.blob_service_client.get_container_client(container)
    total_bytes = sum(blob.size for blob in container_client.list_blobs(name_starts_with=prefix))
    size_mb = total_bytes / (1024 * 1024)
    num_timestamps = int(ds.sizes.get("timestamp", 0))
    var_names = list(ds.data_vars)
    dims_by_var = {v: {d: int(ds[v].sizes[d]) for d in ds[v].dims} for v in var_names}
    lines = [f"size_mb: {size_mb:.2f}", f"sent_mb: {used_sent/(1024*1024):.2f}", f"num_timestamps: {num_timestamps}", f"variables: {', '.join(var_names)}", "dims_by_var:"]
    if repo is not None:
        ic_total_bytes = repo.total_chunks_storage()
        lines.insert(0, f"ic_size_mb: {ic_total_bytes / (1024 * 1024):.2f}")
    for v in var_names:
        dims_str = ", ".join(f"{d}={dims_by_var[v][d]}" for d in ds[v].dims)
        lines.append(f"  - {v}: {dims_str}")
    artifacts.save_text(name, "\n".join(lines) + "\n")
    assert total_bytes > 0


@pytest.mark.parametrize("minimal", [False, True], ids=["full", "minimal"])
def test_single_shot_upload(minimal: bool, artifacts) -> None:
    ds = open_test_dataset()
    if minimal:
        ds = select_minimal_variables(ds)
    ds_hour = _extend_to_hours(ds)

    container = f"single-shot-{'min' if minimal else 'full'}-container"
    prefix = "single-shot-prefix"
    repo, client, _ = _setup_repo(container, prefix)

    start_sent = total_sent_bytes()
    upload_single_chunk(repo, ds_hour)

    _log_stats(ds_hour, artifacts, client, container, prefix, start_sent, name=f"blob_size_{'min' if minimal else 'full'}.txt")


def test_minimal_hour_chunked_upload_incremental(artifacts) -> None:
    """Upload minimal variables in fixed-size chunks, reopening the repo for each append."""

    ds = select_minimal_variables(open_test_dataset())
    ds_hour = clean_dataset(_extend_to_hours(ds))

    chunk_size = 1_000
    total_ts = ds_hour.sizes["timestamp"]
    aligned_ts = (total_ts // chunk_size) * chunk_size
    ds_hour = ds_hour.isel(timestamp=slice(0, aligned_ts))

    encoding = {"timestamp": {"chunks": (chunk_size,)}}
    for v in ds_hour.data_vars:
        if "timestamp" in ds_hour[v].dims:
            shape = ds_hour[v].shape
            encoding[v] = {"chunks": (chunk_size,) + shape[1:]}

    container = "minimal-hour-incremental-container"
    prefix = "minimal-hour-incremental-prefix"
    repo, client, storage = _setup_repo(container, prefix)

    start_sent = total_sent_bytes()

    first_chunk = ds_hour.isel(timestamp=slice(0, chunk_size))
    s = repo.writable_session("main")
    icx.to_icechunk(first_chunk, s, mode="w", encoding=encoding)
    s.commit("initial chunk")

    reopened = icechunk.Repository.open(storage)
    for start in range(chunk_size, aligned_ts, chunk_size):
        s2 = reopened.writable_session("main")
        chunk = ds_hour.isel(timestamp=slice(start, start + chunk_size))
        icx.to_icechunk(chunk, s2, mode="a-", append_dim="timestamp")
        s2.commit("append chunk")

    _log_stats(ds_hour, artifacts, client, container, prefix, start_sent, repo, name="blob_size_incremental.txt")

    reopened = icechunk.Repository.open(storage)
    read_s = reopened.readonly_session("main")
    stored = xr.open_zarr(read_s.store, consolidated=False)
    assert stored.sizes["timestamp"] == aligned_ts
    assert stored["timestamp"].encoding.get("chunks")[0] == chunk_size
    for v in stored.data_vars:
        if "timestamp" in stored[v].dims:
            assert stored[v].encoding.get("chunks")[0] == chunk_size


def test_minimal_hour_chunked_single_manifest_upload_incremental(artifacts) -> None:
    """Upload minimal variables in fixed-size chunks with manifest splitting."""

    ds = select_minimal_variables(open_test_dataset())
    ds_hour = clean_dataset(_extend_to_hours(ds))

    chunk_size = 1_000
    total_ts = ds_hour.sizes["timestamp"]
    aligned_ts = (total_ts // chunk_size) * chunk_size
    ds_hour = ds_hour.isel(timestamp=slice(0, aligned_ts))

    encoding = {"timestamp": {"chunks": (chunk_size,)}}
    for v in ds_hour.data_vars:
        if "timestamp" in ds_hour[v].dims:
            shape = ds_hour[v].shape
            encoding[v] = {"chunks": (chunk_size,) + shape[1:]}

    split_config = ManifestSplittingConfig.from_dict(
        {ManifestSplitCondition.AnyArray(): {ManifestSplitDimCondition.DimensionName("timestamp"): 1}}
    )
    repo_config = icechunk.RepositoryConfig(manifest=icechunk.ManifestConfig(splitting=split_config))

    container = "minimal-hour-manifest-container"
    prefix = "minimal-hour-manifest-prefix"
    repo, client, storage = _setup_repo(container, prefix, repo_config)

    start_sent = total_sent_bytes()

    first_chunk = ds_hour.isel(timestamp=slice(0, chunk_size))
    s = repo.writable_session("main")
    icx.to_icechunk(first_chunk, s, mode="w", encoding=encoding)
    s.commit("initial chunk")

    reopened = icechunk.Repository.open(storage, config=repo_config)
    for start in range(chunk_size, aligned_ts, chunk_size):
        s2 = reopened.writable_session("main")
        chunk = ds_hour.isel(timestamp=slice(start, start + chunk_size))
        icx.to_icechunk(chunk, s2, mode="a-", append_dim="timestamp")
        s2.commit("append chunk")

    repo.expire_snapshots(older_than=datetime.datetime.now(tz=datetime.UTC))
    repo.garbage_collect(datetime.datetime.now(tz=datetime.UTC))

    _log_stats(ds_hour, artifacts, client, container, prefix, start_sent, repo, name="blob_size_incremental.txt")

    reopened = icechunk.Repository.open(storage)
    read_s = reopened.readonly_session("main")
    stored = xr.open_zarr(read_s.store, consolidated=False)
    assert stored.sizes["timestamp"] == aligned_ts
    assert stored["timestamp"].encoding.get("chunks")[0] == chunk_size
    for v in stored.data_vars:
        if "timestamp" in stored[v].dims:
            assert stored[v].encoding.get("chunks")[0] == chunk_size


def test_minimal_hour_timed_single_manifest_upload_incremental(artifacts) -> None:
    """Upload minimal variables in 15-minute increments."""

    ds = select_minimal_variables(open_test_dataset())
    ds_hour = clean_dataset(_extend_to_hours(ds))

    container = "minimal-hour-timed-container"
    prefix = "minimal-hour-timed-prefix"
    repo, client, storage = _setup_repo(container, prefix)

    start_sent = total_sent_bytes()

    ts = ds_hour["timestamp"].values
    step = np.timedelta64(15, "m")
    t0 = ts[0]
    t_last = ts[-1]

    first_end = min(t0 + step, t_last)
    first_end_idx = int(np.searchsorted(ts, first_end, side="right"))
    first_chunk = ds_hour.isel(timestamp=slice(0, first_end_idx))

    s = repo.writable_session("main")
    icx.to_icechunk(first_chunk, s, mode="w")
    s.commit("initial 15-minute window")

    reopened = icechunk.Repository.open(storage)
    cur_start = first_end
    while cur_start < t_last:
        cur_end = cur_start + step
        if cur_end > t_last:
            cur_end = t_last
        start_idx = int(np.searchsorted(ts, cur_start, side="right"))
        end_idx = int(np.searchsorted(ts, cur_end, side="right"))
        if end_idx > start_idx:
            chunk = ds_hour.isel(timestamp=slice(start_idx, end_idx))
            s2 = reopened.writable_session("main")
            icx.to_icechunk(chunk, s2, mode="a-", append_dim="timestamp")
            s2.commit("append 15-minute window")
        cur_start = cur_end

    _log_stats(ds_hour, artifacts, client, container, prefix, start_sent, repo, name="blob_size_incremental.txt")

    reopened = icechunk.Repository.open(storage)
    read_s = reopened.readonly_session("main")
    stored = xr.open_zarr(read_s.store, consolidated=False)
    assert stored.sizes["timestamp"] == ds_hour.sizes["timestamp"]

