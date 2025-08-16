import numpy as np
from pathlib import Path

from tests.helpers import open_test_dataset


def test_generated_repositories_structure(blob_root: Path, generated_repos: list[Path]):
    ds = open_test_dataset()
    instrument = ds.attrs["instrument"]
    project = ds.attrs["project"]
    base_dir = blob_root / instrument / project

    assert base_dir.exists()
    assert len(generated_repos) == 2

    base_ts = np.datetime64(ds["timestamp"].values[0], "s")
    expected = []
    for i in range(2):
        ts = base_ts + np.timedelta64(i, "s")
        ts_str = (
            np.datetime_as_string(ts, unit="s")
            .replace("T", "t")
            .replace(":", "-")
            + "z"
        )
        expected.append(
            base_dir / f"inst-{instrument}-prj-{project}-{ts_str}l1b"
        )

    assert sorted(generated_repos) == sorted(expected)
    for path in expected:
        assert path.is_dir()
