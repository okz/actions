import os
import datetime
import numpy as np
import pytest
import xarray as xr

import icechunk
import icechunk.xarray as icx

from ice_stream.blocks import clean_dataset, select_minimal_variables, upload_single_chunk
from ice_stream.mock_data_generator import generate_mock_data
from icechunk import (
    ManifestSplitCondition,
    ManifestSplittingConfig,
    ManifestSplitDimCondition,
)
from tests.helpers import AzuriteStorageClient, total_sent_bytes, get_test_data_path


# Duration of dataset used in tests (in hours) and chunk size (in minutes).
# These can be overridden via the environment to run longer tests.
TEST_DATA_DURATION_HOURS = int(os.environ.get("TEST_DATA_DURATION_HOURS", 1))
CHUNK_DURATION = np.timedelta64(int(os.environ.get("TEST_CHUNK_DURATION_MINUTES", 15)), "m")
COMPRESSOR = {
    "name": "blosc",
    "configuration": {"cname": "zstd", "clevel": 3},
}

def _generate_dataset_for_hours(hours: int, minimal: bool, artifacts) -> xr.Dataset:
    """Generate a mock dataset of approximately the requested duration.

    Uses the project-wide mock data generator to ensure realistic, randomized
    samples instead of slicing the seed dataset. The generated NetCDF is placed
    under the test's artifact directory for inspection.
    """
    seed = get_test_data_path()
    out_path = artifacts.path / f"mock_{'min' if minimal else 'full'}_{hours}h.nc"
    ds = generate_mock_data(seed, out_path, target_duration_hours=float(hours))
    if minimal:
        ds = select_minimal_variables(ds)
    ds = clean_dataset(ds)
    for name in ds.variables:
        if ds[name].dtype.kind not in {"O", "S"}:
            ds[name].encoding["compressors"] = [COMPRESSOR]
    return ds


def _chunk_size_from_duration(ts: np.ndarray, duration: np.timedelta64 = CHUNK_DURATION) -> int:
    """Return number of samples corresponding to *duration* for a timestamp array."""

    if ts.size < 2:
        return ts.size
    step = ts[1] - ts[0]
    return int(np.ceil(duration / step))


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
    # New: record data duration and chunk duration per time dimension.
    durations_hours: dict[str, float] = {}
    for dim in ("timestamp", "high_res_timestamp"):
        if dim in ds:
            arr = ds[dim].values
            if arr.size >= 2:
                total = arr[-1] - arr[0]
                durations_hours[dim] = float(total / np.timedelta64(1, "h"))
            else:
                durations_hours[dim] = 0.0
    if durations_hours:
        lines.append("data_durations_hours:")
        for dim, dur_h in durations_hours.items():
            lines.append(f"  - {dim}: {dur_h:.4f}")
        # CHUNK_DURATION is the configured time window; log it per present time dim.
        chunk_minutes = float(CHUNK_DURATION / np.timedelta64(1, "m"))
        lines.append("chunk_durations_minutes:")
        for dim in durations_hours.keys():
            lines.append(f"  - {dim}: {chunk_minutes:.0f}")
    artifacts.save_text(name, "\n".join(lines) + "\n")
    assert total_bytes > 0


@pytest.mark.parametrize("minimal", [False, True], ids=["full", "minimal"])
def test_single_shot_upload(minimal: bool, artifacts) -> None:
    # Generate dataset with desired duration using the mock generator
    ds_hour = _generate_dataset_for_hours(TEST_DATA_DURATION_HOURS, minimal, artifacts)

    container = f"single-shot-{'min' if minimal else 'full'}-container"
    prefix = "single-shot-prefix"
    repo, client, _ = _setup_repo(container, prefix)

    start_sent = total_sent_bytes()
    upload_single_chunk(repo, ds_hour)

    _log_stats(ds_hour, artifacts, client, container, prefix, start_sent, name=f"blob_size_{'min' if minimal else 'full'}.txt")


def test_minimal_hour_chunked_upload_incremental(artifacts) -> None:
    """Upload minimal variables in fixed-size chunks, reopening the repo for each append."""
    ds_hour = _generate_dataset_for_hours(TEST_DATA_DURATION_HOURS, minimal=True, artifacts=artifacts)

    ts = ds_hour["timestamp"].values
    chunk_size = _chunk_size_from_duration(ts)
    total_ts = ds_hour.sizes["timestamp"]
    aligned_ts = (total_ts // chunk_size) * chunk_size
    ds_hour = ds_hour.isel(timestamp=slice(0, aligned_ts))

    encoding = {"timestamp": {"chunks": (chunk_size,), "compressors": [COMPRESSOR]}}
    for v in ds_hour.data_vars:
        if "timestamp" in ds_hour[v].dims:
            shape = ds_hour[v].shape
            encoding[v] = {"chunks": (chunk_size,) + shape[1:], "compressors": [COMPRESSOR]}

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
    ds_hour = _generate_dataset_for_hours(TEST_DATA_DURATION_HOURS, minimal=True, artifacts=artifacts)

    ts = ds_hour["timestamp"].values
    chunk_size = _chunk_size_from_duration(ts)
    total_ts = ds_hour.sizes["timestamp"]
    aligned_ts = (total_ts // chunk_size) * chunk_size
    ds_hour = ds_hour.isel(timestamp=slice(0, aligned_ts))

    encoding = {"timestamp": {"chunks": (chunk_size,), "compressors": [COMPRESSOR]}}
    for v in ds_hour.data_vars:
        if "timestamp" in ds_hour[v].dims:
            shape = ds_hour[v].shape
            encoding[v] = {"chunks": (chunk_size,) + shape[1:], "compressors": [COMPRESSOR]}

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


def test_full_dataset_high_freq_chunked_upload(artifacts) -> None:
    """Upload full dataset including high-frequency variables in chunks."""
    ds_hour = _generate_dataset_for_hours(TEST_DATA_DURATION_HOURS, minimal=False, artifacts=artifacts)

    ts = ds_hour["timestamp"].values
    hr_ts = ds_hour["high_res_timestamp"].values
    chunk_size = _chunk_size_from_duration(ts)
    hr_chunk_size = _chunk_size_from_duration(hr_ts)

    total_ts = (ds_hour.sizes["timestamp"] // chunk_size) * chunk_size
    total_hr = (ds_hour.sizes["high_res_timestamp"] // hr_chunk_size) * hr_chunk_size
    ds_hour = ds_hour.isel(timestamp=slice(0, total_ts), high_res_timestamp=slice(0, total_hr))

    encoding = {
        "timestamp": {"chunks": (chunk_size,), "compressors": [COMPRESSOR]},
        "high_res_timestamp": {"chunks": (hr_chunk_size,), "compressors": [COMPRESSOR]},
    }
    for v in ds_hour.data_vars:
        dims = ds_hour[v].dims
        shape = ds_hour[v].shape
        if "timestamp" in dims and 0 not in shape:
            encoding[v] = {"chunks": (chunk_size,) + shape[1:], "compressors": [COMPRESSOR]}
        if "high_res_timestamp" in dims and 0 not in shape:
            encoding[v] = {"chunks": (hr_chunk_size,) + shape[1:], "compressors": [COMPRESSOR]}

    container = "full-highfreq-incremental-container"
    prefix = "full-highfreq-incremental-prefix"
    repo, client, storage = _setup_repo(container, prefix)

    start_sent = total_sent_bytes()

    first_chunk = ds_hour.isel(timestamp=slice(0, chunk_size), high_res_timestamp=slice(0, hr_chunk_size))
    s = repo.writable_session("main")
    icx.to_icechunk(first_chunk, s, mode="w", encoding=encoding)
    s.commit("initial chunk")

    reopened = icechunk.Repository.open(storage)
    num_chunks = max(total_ts // chunk_size, total_hr // hr_chunk_size)
    for i in range(1, num_chunks):
        ts_start = i * chunk_size
        hr_start = i * hr_chunk_size
        s2 = reopened.writable_session("main")
        # Only include variables that depend on the low-frequency timestamp
        chunk_low = ds_hour.isel(timestamp=slice(ts_start, ts_start + chunk_size)).drop_dims(
            "high_res_timestamp", errors="ignore"
        )
        if chunk_low.sizes.get("timestamp", 0) > 0:
            icx.to_icechunk(chunk_low, s2, mode="a-", append_dim="timestamp")
        # Likewise, isolate high-frequency variables when appending along
        # the high_res_timestamp dimension
        chunk_high = ds_hour.isel(high_res_timestamp=slice(hr_start, hr_start + hr_chunk_size)).drop_dims(
            "timestamp", errors="ignore"
        )
        if chunk_high.sizes.get("high_res_timestamp", 0) > 0:
            icx.to_icechunk(chunk_high, s2, mode="a-", append_dim="high_res_timestamp")
        s2.commit("append chunk")

    _log_stats(ds_hour, artifacts, client, container, prefix, start_sent, repo, name="blob_size_full_highfreq.txt")

    reopened = icechunk.Repository.open(storage)
    read_s = reopened.readonly_session("main")
    stored = xr.open_zarr(read_s.store, consolidated=False)
    assert stored.sizes["timestamp"] == total_ts
    assert stored.sizes["high_res_timestamp"] == total_hr
    # ensure high-frequency variables were stored
    for v in ["bearing", "windx", "sonictemp"]:
        if v in stored:
            assert v in stored.data_vars


def test_minimal_hour_timed_single_manifest_upload_incremental(artifacts) -> None:
    """Upload minimal variables in configurable time increments."""
    ds_hour = _generate_dataset_for_hours(TEST_DATA_DURATION_HOURS, minimal=True, artifacts=artifacts)

    container = "minimal-hour-timed-container"
    prefix = "minimal-hour-timed-prefix"
    repo, client, storage = _setup_repo(container, prefix)

    start_sent = total_sent_bytes()

    ts = ds_hour["timestamp"].values
    step = CHUNK_DURATION
    t0 = ts[0]
    t_last = ts[-1]

    first_end = min(t0 + step, t_last)
    first_end_idx = int(np.searchsorted(ts, first_end, side="right"))
    first_chunk = ds_hour.isel(timestamp=slice(0, first_end_idx))

    s = repo.writable_session("main")
    icx.to_icechunk(first_chunk, s, mode="w")
    s.commit("initial window")

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
            s2.commit("append window")
        cur_start = cur_end

    _log_stats(ds_hour, artifacts, client, container, prefix, start_sent, repo, name="blob_size_incremental.txt")

    reopened = icechunk.Repository.open(storage)
    read_s = reopened.readonly_session("main")
    stored = xr.open_zarr(read_s.store, consolidated=False)
    assert stored.sizes["timestamp"] == ds_hour.sizes["timestamp"]
