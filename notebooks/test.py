#%%
import icechunk
import xarray as xr
from pathlib import Path
from dotenv import load_dotenv
load_dotenv(".env")
import os


# Load environment variables from the project root /.env (adjust if necessary)

# Create storage object properly for Azure
storage = icechunk.azure_storage(
    account=os.environ["AZURE_STORAGE_ACCOUNT"],
    container="test",
    prefix="datasets/clads/backups/20250727102904",
    from_env=True,
)

# Open the repository using the storage object
repo = icechunk.Repository.open(storage)

# %%
ro = repo.readonly_session("main")
ds = xr.open_dataset(ro.store, engine="zarr")
print(ds)

# %%
ds   = xr.open_zarr(ro.store)
# %%
ro   = repo.readonly_session("main")
ds   = xr.open_dataset(ro.store, engine="zarr")
print(ds)

# %%
import hvplot.xarray

ds.ch4.isel(timestamp=slice(-100, None)).load().hvplot()


# %%
%%timeit
ds.ch4.sel(timestamp=slice("2025-03-20T00:58:57.478972160", "2025-03-20T23:58:57.478972160")).load()

# %%
ds.windx.high_res_timestamp.values[-10:]


# %%
ds.windx
# %%
len(ds.windx.high_res_timestamp)

# %%
7 * 24 * 60 * 60 / 14
# %%
ds
# %%
ds.windx.high_res_timestamp