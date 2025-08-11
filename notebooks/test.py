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
