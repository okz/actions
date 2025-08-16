#%%
import icechunk
import xarray as xr
from pathlib import Path
from dotenv import load_dotenv
load_dotenv(".env")
import os
import numpy as np
import xarray as xr


# Load environment variables from the project root /.env (adjust if necessary)

# Create storage object properly for Azure
storage = icechunk.azure_storage(
    account=os.environ["AZURE_STORAGE_ACCOUNT"],
    container="test",
    prefix="datasets/clads/backups/20250728052517",
    from_env=True,
)

# Open the repository using the storage object
repo = icechunk.Repository.open(storage)

# %%
ro = repo.readonly_session("main")
ds = xr.open_dataset(ro.store, engine="zarr")
ds

# %%
import hvplot.xarray  # Ensure hvplot is imported for plotting
ds.windx.isel(high_res_timestamp=slice(-100, None)).hvplot.scatter(title="Wind X Component")

# %%
ds
# %%
ancestry = list(repo.ancestry(branch="main"))
print("\n\n".join([str((a.id, a.written_at)) for a in ancestry]))

# %%
expiry_time = ancestry[-2].written_at

results = repo.garbage_collect(expiry_time)
print(results)
# %%
expiry_time
# %%
expired = repo.expire_snapshots(older_than=expiry_time)
print(expired)
# %%
ancestry[-1]
# %%
print(ds)
# %%
print(ds.variables.keys())
# %%
import numpy as np
def compare_datasets(mds: xr.Dataset, ds: xr.Dataset, sample_per_dim: int = 10) -> None:
    if not isinstance(mds, xr.Dataset) or not isinstance(ds, xr.Dataset):
        print("Both mds and ds must be xarray.Dataset")
        return

    # Global attributes
    print("Global attributes:")
    m_only = set(mds.attrs) - set(ds.attrs)
    d_only = set(ds.attrs) - set(mds.attrs)
    changed = {k for k in set(mds.attrs) & set(ds.attrs) if mds.attrs.get(k) != ds.attrs.get(k)}
    if m_only: print("  only in mds:", sorted(m_only))
    if d_only: print("  only in ds:", sorted(d_only))
    if changed: print("  different values:", sorted(changed))

    # Variables present/missing
    print("\nData variables:")
    m_vars = set(mds.data_vars)
    d_vars = set(ds.data_vars)
    if m_vars - d_vars: print("  only in mds:", sorted(m_vars - d_vars))
    if d_vars - m_vars: print("  only in ds:", sorted(d_vars - m_vars))

    # Variable-by-variable structural and sampled value checks
    common_vars = sorted(m_vars & d_vars)
    for name in common_vars:
        va, vb = mds[name], ds[name]
        issues = []
        if va.dims != vb.dims:
            issues.append(f"dims {va.dims} vs {vb.dims}")
        if va.shape != vb.shape:
            issues.append(f"shape {va.shape} vs {vb.shape}")
        if va.dtype != vb.dtype:
            issues.append(f"dtype {va.dtype} vs {vb.dtype}")
        if (va.attrs != vb.attrs):
            issues.append("attrs differ")
        if issues:
            print(f"  {name}: " + "; ".join(issues))
            continue

        # Sampled value comparison to avoid full read
        idx = {}
        for d, n in zip(va.dims, va.shape):
            if n <= sample_per_dim:
                idx[d] = slice(0, n)
            else:
                step = max(n // sample_per_dim, 1)
                idx[d] = slice(0, n, step)

        sa = va.isel(**idx)
        sb = vb.isel(**idx)

        try:
            if np.issubdtype(sa.dtype, np.number) and np.issubdtype(sb.dtype, np.number):
                diff = (sa - sb)
                # finite max abs diff on sample
                mx = np.nanmax(np.abs(diff).values)
                if not (mx == 0 or (np.isnan(mx))):
                    print(f"  {name}: values differ (max abs diff on sample={mx})")
            else:
                neq = (sa.astype("object") != sb.astype("object")).any().item()
                if neq:
                    print(f"  {name}: values differ on sample")
        except Exception as e:
            print(f"  {name}: value check failed: {e}")

    # Coordinates comparison
    print("\nCoordinates:")
    m_coords = set(mds.coords)
    d_coords = set(ds.coords)
    if m_coords - d_coords: print("  only in mds:", sorted(m_coords - d_coords))
    if d_coords - m_coords: print("  only in ds:", sorted(d_coords - m_coords))

    for name in sorted(m_coords & d_coords):
        ca, cb = mds.coords[name], ds.coords[name]
        issues = []
        if ca.dtype != cb.dtype:
            issues.append(f"dtype {ca.dtype} vs {cb.dtype}")
        if ca.shape != cb.shape:
            issues.append(f"shape {ca.shape} vs {cb.shape}")
        if issues:
            print(f"  coord {name}: " + "; ".join(issues))
            continue

        # Sample coordinate value equality (ignoring attrs)
        idx = {}
        for d, n in zip(ca.dims, ca.shape):
            if n <= sample_per_dim:
                idx[d] = slice(0, n)
            else:
                step = max(n // sample_per_dim, 1)
                idx[d] = slice(0, n, step)
        try:
            if (ca.isel(**idx).values != cb.isel(**idx).values).any():
                print(f"  coord {name}: values differ on sample")
        except Exception as e:
            print(f"  coord {name}: check failed: {e}")

# Run comparison
try:
    compare_datasets(mds, ds)
except NameError:
    print("Variable 'mds' is not defined.")
# %%
