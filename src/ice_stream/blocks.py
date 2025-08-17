"""Helpers for working with xarray datasets and Icechunk repositories."""

from __future__ import annotations

from typing import Iterable

import numpy as np
import xarray as xr
import icechunk.xarray as icx
from zarr.registry import register_codec


def clean_dataset(ds: xr.Dataset) -> xr.Dataset:
    """Return a copy with unused coordinates dropped and encodings cleared."""
    used_dims: set[str] = set()
    for var in ds.data_vars:
        used_dims.update(ds[var].dims)
    drop_coords = [c for c in ds.coords if set(ds[c].dims).isdisjoint(used_dims)]
    ds = ds.drop_vars(drop_coords)
    for name in list(ds.variables):
        ds[name].encoding.clear()
        if ds[name].dtype.kind in {"S", "O"}:
            ds[name] = ds[name].astype(str)
            ds[name].encoding.clear()
    return ds


def select_minimal_variables(ds: xr.Dataset) -> xr.Dataset:
    """Return variables with a single dimension excluding high-res timestamps."""
    candidates = [
        v
        for v in ds.data_vars
        if len(ds[v].dims) == 1 and ds[v].dims[0] != "high_res_timestamp"
    ]
    subset = ds[candidates]
    return clean_dataset(subset)


def select_waveform_variables(ds: xr.Dataset, exclude: Iterable[str]) -> xr.Dataset:
    """Variables with ``timestamp`` dimension excluding ``exclude`` and high-res."""
    candidates = [
        v
        for v in ds.data_vars
        if v not in set(exclude)
        and "timestamp" in ds[v].dims
        and "high_res_timestamp" not in ds[v].dims
    ]
    subset = ds[candidates]
    return clean_dataset(subset)


def select_high_freq_variables(ds: xr.Dataset) -> xr.Dataset:
    """Variables that use the ``high_res_timestamp`` dimension."""
    candidates = [v for v in ds.data_vars if "high_res_timestamp" in ds[v].dims]
    subset = ds[candidates]
    return clean_dataset(subset)


def upload_in_intervals(
    repo: "icechunk.Repository",
    ds: xr.Dataset,
    dim: str,
    interval: np.timedelta64,
    mode_first: str = "w",
    encoding: dict[str, dict[str, object]] | None = None,
) -> None:
    """Upload *ds* to *repo* in chunks along *dim* with given *interval*.

    Parameters
    ----------
    repo : icechunk.Repository
        Destination repository.
    ds : xr.Dataset
        Dataset to upload.
    dim : str
        Dimension along which to chunk and append.
    interval : np.timedelta64
        Time window represented by each chunk.
    mode_first : str, optional
        Icechunk write mode used for the first chunk, by default "w".
    encoding : dict[str, dict[str, object]], optional
        Explicit encoding map passed to :func:`icechunk.xarray.to_icechunk`.
        If omitted, chunk encodings are inferred from the first interval so
        that variables (including the coordinate for ``dim``) share a consistent
        chunk size during subsequent appends.
    """

    start = ds[dim].values[0]
    end = ds[dim].values[-1]
    first_end = start + interval
    first_slice = ds.sel({dim: slice(start, first_end)})
    if encoding is None:
        chunk_size = first_slice.sizes[dim]
        encoding = {}
        for name in ds.variables:
            if dim in ds[name].dims:
                shape = ds[name].shape
                encoding[name] = {"chunks": (chunk_size,) + shape[1:]}
    session = repo.writable_session("main")
    icx.to_icechunk(first_slice, session, mode=mode_first, encoding=encoding)
    session.commit("initial chunk")

    current = first_end
    while current < end:
        next_t = current + interval
        chunk = ds.sel({dim: slice(current, next_t)})
        if chunk.sizes.get(dim, 0) > 0:
            session = repo.writable_session("main")
            icx.to_icechunk(chunk, session, mode="a", append_dim=dim, encoding=encoding)
            session.commit("append chunk")
        current = next_t


def upload_single_chunk(repo: "icechunk.Repository", ds: xr.Dataset, message: str = "single chunk") -> None:
    """Upload the entire dataset to the repository in one commit."""
    session = repo.writable_session("main")
    icx.to_icechunk(ds, session, mode="w")
    session.commit(message)


class _NullCodec:
    codec_id = "null"

    def encode(self, buf):  # type: ignore[override]
        return buf

    def decode(self, buf, out=None):  # type: ignore[override]
        return buf


def ensure_null_codec() -> None:
    """Register a no-op codec under the name ``null`` if missing."""
    try:
        register_codec(_NullCodec())
    except Exception:
        pass
