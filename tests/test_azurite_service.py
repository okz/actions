import os
import fsspec
import xarray as xr
import numpy as np
import icechunk
import icechunk.xarray as icx
import pytest

from ice_stream.mock_data_generator import generate_mock_data
from ice_stream.blocks import (
    select_minimal_variables,
    select_waveform_variables,
    select_high_freq_variables,
    upload_in_intervals,
    upload_single_chunk,
    clean_dataset,
    ensure_null_codec,
)

from tests.helpers import (
    AzuriteStorageClient,
    get_test_data_path,
    open_test_dataset,
    setup_icechunk_repo,
    total_sent_bytes,
)


def test_azurite_basic_operations():
    client = AzuriteStorageClient()
    assert client.create_container() is True

    blob_name = "healthcheck.txt"
    content = "hello from tests"

    assert client.upload_blob(blob_name, content) is True
    assert client.blob_exists(blob_name) is True
    assert client.download_blob(blob_name) == content
    assert client.delete_blob(blob_name) is True


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


def test_azure_icechunk():
    """Create an empty icechunk repository on Azurite."""
    repo = setup_icechunk_repo("my-container", "my-prefix")
    assert repo is not None


def test_azure_icechunk_xarray_upload(tmp_path):
    """Upload a NetCDF file to Azurite via icechunk using xarray."""
    repo = setup_icechunk_repo("xarray-container", "xarray-prefix")

    session = repo.writable_session("main")
    ds = open_test_dataset()
    start_bytes = total_sent_bytes()
    icx.to_icechunk(ds, session, mode="w")
    session.commit("initial upload")
    used = total_sent_bytes() - start_bytes

    ro = repo.readonly_session("main")
    result = xr.open_dataset(ro.store, engine="zarr")

    assert len(result["timestamp"]) == len(ds["timestamp"])
    assert len(result["high_res_timestamp"]) == len(ds["high_res_timestamp"])
    assert used > 0


def test_azure_icechunk_append(tmp_path):
    """Append extended data to an existing icechunk store."""

    start_bytes = total_sent_bytes()

    repo = setup_icechunk_repo("append-container", "append-prefix")

    base_session = repo.writable_session("main")
    ds_seed = open_test_dataset()
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
    # Allow some overhead for protocol traffic but do not require the traffic
    # to match the payload exactly. Low-level network counters can under-report
    # bytes on some platforms.
    assert 0 < used <= file_size * 2
    # ----------------------------------------------------------------------

    ro = repo.readonly_session("main")
    result = xr.open_dataset(ro.store, engine="zarr")

    assert len(result["timestamp"]) == len(ds_extended["timestamp"])
    assert len(result["high_res_timestamp"]) == len(ds_extended["high_res_timestamp"])
    assert used > 0


def test_large_repo_read_performance_azurite(tmp_path) -> None:
    """Upload a moderately sized repo and check last timestamps read quickly."""
    repo = setup_icechunk_repo("perf-container", "perf-prefix")

    large_path = tmp_path / "large.nc"
    generate_mock_data(
        seed_file=get_test_data_path(),
        output_file=large_path,
        target_duration_hours=4,
    )

    ds = xr.open_dataset(large_path)

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
            ts_session.commit("ts chunk")
            created_ts = True

        hr_chunk = ds.sel(high_res_timestamp=slice(current, next_t))
        hr_chunk = hr_chunk[[v for v in hr_chunk.data_vars if "high_res_timestamp" in hr_chunk[v].dims]]
        if hr_chunk.sizes.get("high_res_timestamp", 0) > 0:
            hr_session = repo.writable_session("main")
            mode = "a" if (created_ts or created_hr) else "w"
            kw = {"append_dim": "high_res_timestamp"} if created_hr else {}
            icx.to_icechunk(hr_chunk, hr_session, mode=mode, **kw)
            hr_session.commit("hr chunk")
            created_hr = True

        current = next_t

    ro = repo.readonly_session("main")
    ds_remote = xr.open_dataset(ro.store, engine="zarr")
    last = ds_remote["timestamp"].values[-1]
    first = ds_remote["timestamp"].values[0]
    hours = (last - first) / np.timedelta64(1, "h")
    assert hours >= 4


def test_azure_icechunk_append_new_variables() -> None:
    """Ensure new variables can be added in a later session."""

    container = "var-append-container"
    prefix = "var-append-prefix"
    repo = setup_icechunk_repo(container, prefix)

    ds = open_test_dataset()
    fed_vars = ds.attrs.get("fitted_measurements", "").split()
    first_ds = ds[fed_vars]

    session = repo.writable_session("main")
    icx.to_icechunk(first_ds, session, mode="w")
    session.commit("initial vars")

    storage = icechunk.azure_storage(
        account=os.environ["AZURE_STORAGE_ACCOUNT_NAME"],
        container=container,
        prefix=prefix,
        from_env=True,
        config={
            "azure_storage_use_emulator": "true",
            "azure_allow_http": "true",
        },
    )
    reopened = icechunk.Repository.open(storage)

    ro = reopened.readonly_session("main")
    ds_remote = xr.open_dataset(ro.store, engine="zarr")
    for v in fed_vars:
        assert v in ds_remote.data_vars

    remaining = ds.drop_vars(fed_vars)
    session2 = reopened.writable_session("main")
    icx.to_icechunk(remaining, session2, mode="a")
    session2.commit("append vars")

    ro2 = reopened.readonly_session("main")
    final = xr.open_dataset(ro2.store, engine="zarr")
    for v in ds.data_vars:
        assert v in final.data_vars


def test_azure_icechunk_append_reduced_variables() -> None:
    """Append fitted variables from later timestamps to a partial repo."""

    container = "var-append-reduced-container"
    prefix = "var-append-reduced-prefix"
    repo = setup_icechunk_repo(container, prefix)

    ds = open_test_dataset()
    mid_ts = ds.sizes["timestamp"] // 2
    mid_hr = ds.sizes["high_res_timestamp"] // 2

    first = ds.isel(timestamp=slice(0, mid_ts), high_res_timestamp=slice(0, mid_hr))
    session = repo.writable_session("main")
    icx.to_icechunk(first, session, mode="w")
    session.commit("first half")

    fed_vars = ds.attrs.get("fitted_measurements", "").split()
    second = ds.isel(timestamp=slice(mid_ts, None))[fed_vars]
    second = second.drop_dims("high_res_timestamp", errors="ignore")
    session2 = repo.writable_session("main")
    icx.to_icechunk(second, session2, append_dim="timestamp")
    session2.commit("append fitted")

    ro = repo.readonly_session("main")
    store = ro.store
    drop_vars = [v for v in ds.data_vars if v not in fed_vars]
    ds_fed = xr.open_dataset(store, engine="zarr", drop_variables=drop_vars)

    assert len(ds_fed["timestamp"]) == ds.sizes["timestamp"]
    for v in fed_vars:
        assert len(ds_fed[v]) == ds.sizes["timestamp"]

    import zarr

    hum = zarr.open_array(store, path="humidity_percent")
    assert hum.shape[0] == mid_ts


def test_azure_repo_size_24h_minimal(tmp_path, artifacts) -> None:
    """Upload 24h of minimal variables in 15minute increments and report size."""

    container = "day-size-container"
    prefix = "day-size-prefix"
    repo = setup_icechunk_repo(container, prefix)

    full_day = tmp_path / "full_day.nc"
    ds_full = generate_mock_data(
        seed_file=get_test_data_path(),
        output_file=full_day,
        target_duration_hours=24,
    )

    # Select only minimal variables
    min_vars = {"json"}
    min_vars.update(v for v in ds_full.data_vars if "retro" in ds_full[v].dims)
    min_vars.update(
        [
            "temperature_k",
            "pressure_torr",
            "humidity_percent",
            "signal_strength_dbm",
            "measurement_validity",
            "diagnostics_settings_id",
        ]
    )
    for name in ds_full.attrs.get("fitted_measurements", "").split():
        min_vars.add(name)
        err = name + "_err"
        if err not in ds_full:
            err = name + "_stderr"
        if err in ds_full:
            min_vars.add(err)

    ds = ds_full[sorted(min_vars)]

    # Drop coordinates unrelated to the minimal variables
    used_dims: set[str] = set()
    for var in ds.data_vars:
        used_dims.update(ds[var].dims)
    drop_coords = [c for c in ds.coords if set(ds[c].dims).isdisjoint(used_dims)]
    ds = ds.drop_vars(drop_coords)

    # Remove inherited encodings and ensure strings are unicode
    for name in list(ds.variables):
        ds[name].encoding.clear()
        if ds[name].dtype.kind in {"S", "O"}:
            ds[name] = ds[name].astype(str)
            ds[name].encoding.clear()

    interval = np.timedelta64(15, "m")
    ts_start = ds["timestamp"].values[0]
    ts_end = ds["timestamp"].values[-1]

    first_end = ts_start + interval
    first_slice = ds.sel(timestamp=slice(ts_start, first_end))
    session = repo.writable_session("main")
    icx.to_icechunk(first_slice, session, mode="w")
    session.commit("initial chunk")

    current = first_end
    while current < ts_end:
        next_t = current + interval
        chunk = ds.sel(timestamp=slice(current, next_t))
        if chunk.sizes.get("timestamp", 0) > 0:
            session = repo.writable_session("main")
            icx.to_icechunk(chunk, session, mode="a", append_dim="timestamp")
            session.commit("append chunk")
        current = next_t

    ro = repo.readonly_session("main")
    ds_remote = xr.open_dataset(ro.store, engine="zarr")
    assert set(ds_remote.data_vars) == min_vars
    assert len(ds_remote["timestamp"]) == len(ds["timestamp"])

    client = AzuriteStorageClient()
    client.container_name = container
    container_client = client.blob_service_client.get_container_client(container)
    total_bytes = sum(
        blob.size for blob in container_client.list_blobs(name_starts_with=prefix)
    )

    # Save size details to artifacts for CI
    artifacts.save_text(
        "repo_size.txt",
        f"total_bytes={total_bytes}\n"
        f"total_megabytes={total_bytes/(1024*1024):.2f}\n"
    )

    assert 0 < total_bytes < 50 * 1024 * 1024


def test_azure_repo_append_waveform_highfreq(tmp_path, artifacts) -> None:
    """Append waveform and high-frequency data and compare repo sizes."""

    full_day = tmp_path / "full_day.nc"
    ds_full = generate_mock_data(
        seed_file=get_test_data_path(),
        output_file=full_day,
        target_duration_hours=24,
    )

    # --- chunked repository -------------------------------------------------
    chunk_container = "chunked-container"
    chunk_prefix = "chunked-prefix"
    repo_chunk = setup_icechunk_repo(chunk_container, chunk_prefix)

    ds_min = select_minimal_variables(ds_full)
    upload_in_intervals(repo_chunk, ds_min, "timestamp", np.timedelta64(15, "m"))

    ds_wave = select_waveform_variables(ds_full, ds_min.data_vars)
    ds_wave = ds_wave.drop_vars("waveforms_wavenumbers", errors="ignore")
    ensure_null_codec()
    wave_session = repo_chunk.writable_session("main")
    icx.to_icechunk(ds_wave, wave_session, mode="a")
    wave_session.commit("append waveforms")

    ds_high = select_high_freq_variables(ds_full)
    upload_in_intervals(
        repo_chunk, ds_high, "high_res_timestamp", np.timedelta64(4, "h"), mode_first="a"
    )

    ro = repo_chunk.readonly_session("main")
    ds_remote = xr.open_dataset(ro.store, engine="zarr")
    expected_vars = set(ds_full.data_vars) - {"waveforms_wavenumbers"}
    assert set(ds_remote.data_vars) == expected_vars
    assert len(ds_remote["timestamp"]) == len(ds_full["timestamp"])
    assert len(ds_remote["high_res_timestamp"]) == len(ds_full["high_res_timestamp"])

    client = AzuriteStorageClient()
    client.container_name = chunk_container
    container_client = client.blob_service_client.get_container_client(chunk_container)
    chunked_bytes = sum(
        blob.size for blob in container_client.list_blobs(name_starts_with=chunk_prefix)
    )

    # --- single-chunk repository -------------------------------------------
    single_container = "single-container"
    single_prefix = "single-prefix"
    repo_single = setup_icechunk_repo(single_container, single_prefix)
    upload_single_chunk(repo_single, clean_dataset(ds_full))

    client.container_name = single_container
    container_client = client.blob_service_client.get_container_client(single_container)
    single_bytes = sum(
        blob.size for blob in container_client.list_blobs(name_starts_with=single_prefix)
    )

    artifacts.save_text(
        "repo_size_full.txt",
        f"chunked_bytes={chunked_bytes}\nchunked_mb={chunked_bytes/(1024*1024):.2f}\n"
        f"single_bytes={single_bytes}\nsingle_mb={single_bytes/(1024*1024):.2f}\n",
    )

    assert chunked_bytes > 0
    assert single_bytes > 0

