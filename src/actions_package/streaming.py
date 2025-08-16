from __future__ import annotations
from dataclasses import dataclass
from typing import Iterable, Optional, Dict, List
from pathlib import Path
import argparse, os, tempfile, hashlib, json

import numpy as np
import pandas as pd
import xarray as xr
import icechunk

# ----------------------------- config ----------------------------------------

@dataclass
class StreamConfig:
    connection_string: str
    container: str
    prefix: str
    branch: str = "main"
    time_coord: str = "timestamp"
    significant_keys: tuple[str, ...] = ("instrument", "settings_id")  # tweak as needed



# --------------------------- repo helpers ------------------------------------

def open_or_create_repo(cfg: StreamConfig) -> icechunk.Repository:
    storage = icechunk.azure_storage(
        connection_string=cfg.connection_string,
        container=cfg.container,
        prefix=cfg.prefix,
    )
    try:
        return icechunk.Repository.open(storage)
    except Exception:
        return icechunk.Repository.create(storage)


def latest_timestamp(repo: icechunk.Repository, cfg: StreamConfig) -> Optional[pd.Timestamp]:
    try:
        with repo.readonly_session(branch=cfg.branch) as sess:
            ds = xr.open_zarr(sess.store, consolidated=False)
            if cfg.time_coord not in ds:
                return None
            return pd.Timestamp(pd.to_datetime(ds[cfg.time_coord].values[-1]))
    except Exception:
        return None



def discover_window(repo: icechunk.Repository, cfg: StreamConfig,
                    since_hint: Optional[pd.Timestamp],
                    until_hint: Optional[pd.Timestamp]) -> tuple[pd.Timestamp, pd.Timestamp]:
    now = pd.Timestamp.utcnow()
    last = latest_timestamp(repo, cfg)
    since = (last + pd.Timedelta(milliseconds=1)) if last is not None else now.normalize()
    if since_hint and since_hint > since:
        since = since_hint
    until = min(until_hint or now, now)  # never beyond "now"
    return since, until

# --------------------- mock “backup provider” --------------------------------
def mock_get_backup_files(tmpdir: Path, since: pd.Timestamp, until: pd.Timestamp,
                          time_coord: str = "timestamp") -> list[Path]:
    paths: list[Path] = []
    def mk_ds(t0: pd.Timestamp, n: int) -> xr.Dataset:
        times = pd.date_range(t0, periods=max(1, n), freq="min")
        return xr.Dataset(
            data_vars={
                "wave":   (time_coord, np.random.rand(times.size)),
                "scalar": (time_coord, np.random.randn(times.size)),
            },
            coords={time_coord: times},
            attrs={"instrument": "mock-001", "settings_id": 1},
        )
    mid = since + (until - since) / 2
    slices = [
        mk_ds(since, int((mid - since).total_seconds() // 60)),
        mk_ds(mid,   int((until - mid).total_seconds() // 60)),
    ]
    for i, ds in enumerate(slices, 1):
        p = tmpdir / f"slice_{i}.zarr"
        ds.to_zarr(p, mode="w")
        paths.append(p)
    return paths

# --------------------------- change tagging ----------------------------------

def attrs_fingerprint(attrs: Dict[str, object], keys: tuple[str, ...]) -> str:
    payload = {k: attrs.get(k) for k in keys}
    return hashlib.sha1(json.dumps(payload, sort_keys=True).encode()).hexdigest()[:12]

def get_last_attrs(repo: icechunk.Repository, cfg: StreamConfig) -> Dict[str, object]:
    try:
        with repo.readonly_session(branch=cfg.branch) as sess:
            ds = xr.open_zarr(sess.store, consolidated=False)
            return dict(ds.attrs)
    except Exception:
        return {}

def maybe_tag_settings_change(repo: icechunk.Repository, cfg: StreamConfig,
                              prev_attrs: Dict[str, object], new_attrs: Dict[str, object],
                              snapshot_id: str) -> None:
    prev_fp = attrs_fingerprint(prev_attrs, cfg.significant_keys) if prev_attrs else None
    new_fp  = attrs_fingerprint(new_attrs,  cfg.significant_keys)
    if prev_fp != new_fp:
        tag = f"settings-{new_fp}-start"
        # idempotent-ish: only create if absent
        try:
            repo.create_tag(tag, snapshot_id=snapshot_id)
        except Exception:
            pass  # tag may already exist; safe to ignore

# ------------------------------ ingest ---------------------------------------

def ingest_paths(repo: icechunk.Repository, cfg: StreamConfig, zarr_paths: Iterable[Path],
                 message: str) -> str:
    prev_attrs = get_last_attrs(repo, cfg)
    with repo.writable_session(cfg.branch) as sess:
        last_ds_attrs: Dict[str, object] = prev_attrs
        for p in zarr_paths:
            ds = xr.open_zarr(p, consolidated=False)
            ds.to_zarr(sess.store, region="auto", consolidated=False)
            last_ds_attrs = dict(ds.attrs)  # keep attrs from the last slice
        snap = sess.commit(message)
    maybe_tag_settings_change(repo, cfg, prev_attrs, last_ds_attrs, snap)
    return snap

# ---------------------------------- CLI --------------------------------------

def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser("Icechunk streamer (no day cut-off)")
    ap.add_argument("--container", required=True)
    ap.add_argument("--prefix", required=True)
    ap.add_argument("--since-hint")
    ap.add_argument("--until-hint")
    ap.add_argument("--branch", default="main")
    ap.add_argument("--time-coord", default="timestamp")
    ap.add_argument("--sig-keys", default="instrument,settings_id",
                    help="comma-separated significant attrs for change tagging")
    return ap.parse_args()

def main() -> None:
    args = parse_args()
    conn = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
    if not conn:
        raise RuntimeError("AZURE_STORAGE_CONNECTION_STRING not set")
    cfg = StreamConfig(
        connection_string=conn,
        container=args.container,
        prefix=args.prefix,
        branch=args.branch,
        time_coord=args.time_coord,
        significant_keys=tuple(k.strip() for k in args.sig-keys.split(","))  # type: ignore[attr-defined]
    )

    repo = open_or_create_repo(cfg)
    since = pd.Timestamp(args.since_hint) if args.since_hint else None
    until = pd.Timestamp(args.until_hint) if args.until_hint else None
    since, until = discover_window(repo, cfg, since, until)

    with tempfile.TemporaryDirectory() as td:
        paths = mock_get_backup_files(Path(td), since, until, cfg.time_coord)
        snap = ingest_paths(repo, cfg, paths, f"ingest {since}..{until}")

    print(f"Committed snapshot {snap} on branch {cfg.branch}")

if __name__ == "__main__":
    main()
