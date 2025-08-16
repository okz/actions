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

    date = np.datetime_as_string(ds["timestamp"].values[0], unit="D").replace("-", "")
    expected = [
        base_dir / f"{date}_{int(ds.attrs['gas_id']) + i}_{int(ds.attrs['gas_version']) + i}"
        for i in range(2)
    ]

    assert sorted(generated_repos) == sorted(expected)
    for path in expected:
        assert path.is_dir()
