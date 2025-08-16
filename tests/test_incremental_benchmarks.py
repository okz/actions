import json
import os
import time
import datetime as dt
from typing import Callable, Mapping

import numpy as np
import psutil
import xarray as xr
import icechunk
import icechunk.xarray as icx
import pytest

from ice_stream.blocks import select_minimal_variables, clean_dataset
from tests.helpers import AzuriteStorageClient, open_test_dataset


# -----------------------------------------------------------------------------
# Utility helpers
# -----------------------------------------------------------------------------

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


def _measure_repo_size(container_client, prefix: str) -> int:
    return sum(blob.size for blob in container_client.list_blobs(name_starts_with=prefix))


def _run_incremental(
    name: str,
    artifacts,
    *,
    encoding_fn: Callable[[xr.Dataset], Mapping[str, Mapping]] | None = None,
    repo_config: Mapping | None = None,
    retention: Callable[[icechunk.Repository], None] | None = None,
) -> None:
    """Run an incremental upload benchmark and log metrics."""
    ds = open_test_dataset()
    ds_min = select_minimal_variables(ds)
    ds_day = _extend_to_24h(ds_min)
    ds_day = clean_dataset(ds_day)

    encoding = encoding_fn(ds_day) if encoding_fn else None

    container = f"{name}-container"
    prefix = f"{name}-prefix"
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

    repo_cfg_obj = (
        icechunk.RepositoryConfig(**repo_config) if repo_config else None
    )
    if repo_cfg_obj:
        repo = icechunk.Repository.create(storage, config=repo_cfg_obj)
    else:
        repo = icechunk.Repository.create(storage)

    t_vals = ds_day["timestamp"].values
    interval = np.timedelta64(15, "m")
    t0 = t_vals[0]
    tN = t_vals[-1]
    first_end = t0 + interval

    container_client = client.blob_service_client.get_container_client(container)
    blob_state: dict[str, str] = {}
    commit_metrics: list[dict[str, float | int]] = []

    net_start = psutil.net_io_counters()

    first_chunk = ds_day.sel(timestamp=slice(t0, first_end))
    session = repo.writable_session("main")
    icx.to_icechunk(first_chunk, session, mode="w", encoding=encoding)
    tcs = time.perf_counter()
    session.commit("initial chunk")
    tce = time.perf_counter()
    blob_state = {b.name: b.etag for b in container_client.list_blobs(name_starts_with=prefix)}
    commit_metrics.append({"latency": tce - tcs, "updated": len(blob_state)})

    current = first_end
    while current < tN:
        next_t = current + interval
        chunk = ds_day.sel(timestamp=slice(current, next_t))
        if chunk.sizes.get("timestamp", 0) > 0:
            if repo_cfg_obj:
                repo = icechunk.Repository.open(storage, config=repo_cfg_obj)
            else:
                repo = icechunk.Repository.open(storage)
            s2 = repo.writable_session("main")
            icx.to_icechunk(chunk, s2, mode="a", append_dim="timestamp", encoding=encoding)
            tcs = time.perf_counter()
            s2.commit("append chunk")
            tce = time.perf_counter()
            new_state = {
                b.name: b.etag
                for b in container_client.list_blobs(name_starts_with=prefix)
            }
            updated = sum(
                1 for k, v in new_state.items() if blob_state.get(k) != v
            )
            blob_state = new_state
            commit_metrics.append({"latency": tce - tcs, "updated": updated})
            if retention:
                retention(repo)
        current = next_t

    net_end = psutil.net_io_counters()
    bytes_out = max(0, net_end.bytes_sent - net_start.bytes_sent)
    bytes_in = max(0, net_end.bytes_recv - net_start.bytes_recv)

    repo_size = _measure_repo_size(container_client, prefix)

    if repo_cfg_obj:
        repo = icechunk.Repository.open(storage, config=repo_cfg_obj)
    else:
        repo = icechunk.Repository.open(storage)
    ro_session = repo.readonly_session("main")
    store = ro_session.store
    t_read_start = time.perf_counter()
    ds_read = xr.open_zarr(store)
    ds_read.sel(timestamp=slice(ds_read["timestamp"].values[-1] - np.timedelta64(24, "h"), None))
    read_latency = time.perf_counter() - t_read_start

    result = {
        "repo_size": repo_size,
        "bytes_out": bytes_out,
        "bytes_in": bytes_in,
        "read_latency": read_latency,
        "commit_metrics": commit_metrics,
    }
    artifacts.save_text(f"{name}.json", json.dumps(result, indent=2))
    assert repo_size > 0


# -----------------------------------------------------------------------------
# Sharding benchmarks
# -----------------------------------------------------------------------------

def _shard_encoding(size: int) -> Callable[[xr.Dataset], Mapping[str, Mapping]]:
    def encoder(ds: xr.Dataset) -> Mapping[str, Mapping]:
        enc: dict[str, Mapping] = {}
        for v in ds.data_vars:
            if "timestamp" in ds[v].dims:
                enc[v] = {"chunks": (size,)}
        return enc

    return encoder


@pytest.mark.parametrize(
    "scenario, enc_fn",
    [
        ("S1", None),
        ("S2", _shard_encoding(1)),
        ("S3", _shard_encoding(4)),
    ],
)
def test_sharding_benchmarks(artifacts, scenario, enc_fn) -> None:
    _run_incremental(f"shard_{scenario}", artifacts, encoding_fn=enc_fn)


# -----------------------------------------------------------------------------
# Manifest / metadata benchmarks
# -----------------------------------------------------------------------------

@pytest.mark.parametrize(
    "scenario, config",
    [
        ("M1", {}),
        ("M2", {"manifest": {"split": True, "preload": True}}),
        ("M3_0", {"inline_chunk_threshold_bytes": 0}),
        ("M3_16K", {"inline_chunk_threshold_bytes": 16 * 1024}),
        ("M3_64K", {"inline_chunk_threshold_bytes": 64 * 1024}),
    ],
)
def test_manifest_benchmarks(artifacts, scenario, config) -> None:
    _run_incremental(f"manifest_{scenario}", artifacts, repo_config=config)


# -----------------------------------------------------------------------------
# Snapshot retention / GC benchmarks
# -----------------------------------------------------------------------------

def _retention_last24(repo: icechunk.Repository) -> None:
    cutoff = dt.datetime.utcnow() - dt.timedelta(hours=24)
    repo.expire_snapshots(cutoff)
    repo.garbage_collect(dt.datetime.utcnow())


def _retention_last2h(repo: icechunk.Repository) -> None:
    cutoff = dt.datetime.utcnow() - dt.timedelta(hours=2)
    repo.expire_snapshots(cutoff)
    repo.garbage_collect(dt.datetime.utcnow())


@pytest.mark.parametrize(
    "scenario, retain",
    [
        ("R1", None),
        ("R2", _retention_last24),
        ("R3", _retention_last2h),
    ],
)
def test_retention_benchmarks(artifacts, scenario, retain) -> None:
    _run_incremental(f"retention_{scenario}", artifacts, retention=retain)
