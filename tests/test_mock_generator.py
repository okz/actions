import pytest
import xarray as xr
import numpy as np
from pathlib import Path
import tempfile
import os
import pandas as pd
from tests.helpers import open_test_dataset

from ice_stream.mock_data_generator import generate_mock_data


class TestMockDataGenerator:
    
    @pytest.fixture
    def seed_file(self):
        """Path to the seed data file"""
        return Path(__file__).resolve().parent / "data" / "small_data.zarr.zip"
    
    @pytest.fixture
    def temp_output_file(self):
        """Create a temporary output file"""
        fd, path = tempfile.mkstemp(suffix='.nc')
        os.close(fd)
        yield Path(path)
        # Cleanup
        if Path(path).exists():
            Path(path).unlink()

    @pytest.fixture
    def high_freq_seed_file(self, tmp_path):
        """Create a temporary high frequency dataset"""
        timestamps = pd.date_range("2020-01-01", periods=2, freq="1S")
        high_res = pd.date_range("2020-01-01", periods=4, freq="250ms")
        ds = xr.Dataset(
            {
                "ts_var": ("timestamp", np.arange(len(timestamps))),
                "hf_var": ("high_res_timestamp", np.arange(len(high_res))),
            },
            coords={"timestamp": timestamps, "high_res_timestamp": high_res},
        )
        path = tmp_path / "high_freq.nc"
        ds.to_netcdf(path)
        return path
    
    def test_generate_by_file_size(self, seed_file, temp_output_file):
        """Test generating mock data by target file size"""
        # Generate a 50MB file from 5MB seed
        ds_mock = generate_mock_data(
            seed_file=seed_file,
            output_file=temp_output_file,
            target_size_mb=50
        )
        
        # Verify the dataset was created
        assert temp_output_file.exists()
        
        # Load original dataset for comparison
        ds_seed = open_test_dataset()
        
        # Check that timestamps were extended
        assert len(ds_mock['timestamp']) > len(ds_seed['timestamp'])
        assert len(ds_mock['high_res_timestamp']) > len(ds_seed['high_res_timestamp'])
        
        # Verify timestamps are monotonic and unique
        assert np.all(np.diff(ds_mock['timestamp'].values) > np.timedelta64(0, 'ns'))
        assert np.all(np.diff(ds_mock['high_res_timestamp'].values) > np.timedelta64(0, 'ns'))

        # Check that data variables were extended appropriately
        for var in ds_seed.data_vars:
            if 'timestamp' in ds_seed[var].dims:
                assert ds_mock[var].shape[0] > ds_seed[var].shape[0]

        # Ratio between high-res and normal timestamps should remain constant
        ratio_seed = len(ds_seed['high_res_timestamp']) / len(ds_seed['timestamp'])
        ratio_mock = len(ds_mock['high_res_timestamp']) / len(ds_mock['timestamp'])
        assert ratio_mock == ratio_seed
        
        # Verify file size is approximately what we requested
        actual_size_mb = temp_output_file.stat().st_size / (1024 * 1024)
        assert actual_size_mb >= 45  # Allow some tolerance
    
    def test_generate_by_duration(self, seed_file, temp_output_file):
        """Test generating mock data by target duration"""
        # Generate 24 hours of data
        ds_mock = generate_mock_data(
            seed_file=seed_file,
            output_file=temp_output_file,
            target_duration_hours=24
        )
        ds_seed = open_test_dataset()
        
        # Calculate actual duration
        duration = ds_mock['timestamp'].values[-1] - ds_mock['timestamp'].values[0]
        duration_hours = duration / np.timedelta64(1, 'h')
        
        # Verify duration is at least what we requested
        assert duration_hours >= 24
        
        # Verify high resolution timestamps also extended properly
        high_res_duration = ds_mock['high_res_timestamp'].values[-1] - ds_mock['high_res_timestamp'].values[0]
        high_res_duration_hours = high_res_duration / np.timedelta64(1, 'h')
        # Some datasets may have shorter high frequency spans, ensure monotonic
        assert high_res_duration_hours > 0

        ratio_seed = len(ds_seed['high_res_timestamp']) / len(ds_seed['timestamp'])
        ratio_mock = len(ds_mock['high_res_timestamp']) / len(ds_mock['timestamp'])
        assert ratio_mock == ratio_seed
    
    def test_add_retro_ids(self, seed_file, temp_output_file):
        """Test adding new retro IDs"""
        # Add new retro IDs
        new_retro_ids = [700, 701, 702]
        
        ds_mock = generate_mock_data(
            seed_file=seed_file,
            output_file=temp_output_file,
            target_size_mb=10,
            additional_retro_ids=new_retro_ids
        )
        
        # Load original dataset
        ds_seed = open_test_dataset()
        
        # Verify retro dimension was extended
        assert len(ds_mock['retro']) == len(ds_seed['retro']) + len(new_retro_ids)
        
        # Check that new retro IDs are present
        for retro_id in new_retro_ids:
            assert retro_id in ds_mock['retro'].values
        
        # Verify retro coordinates were extended
        assert len(ds_mock['retro_latitude']) == len(ds_mock['retro'])
        assert len(ds_mock['retro_longitude']) == len(ds_mock['retro'])
        assert len(ds_mock['retro_altitude_m']) == len(ds_mock['retro'])
        
        # Check that retro values are unique
        assert len(np.unique(ds_mock['retro'].values)) == len(ds_mock['retro'].values)
    
    def test_invalid_parameters(self, seed_file, temp_output_file):
        """Test that invalid parameter combinations raise errors"""
        # Both size and duration specified
        with pytest.raises(ValueError, match="Only one of"):
            generate_mock_data(
                seed_file=seed_file,
                output_file=temp_output_file,
                target_size_mb=50,
                target_duration_hours=24
            )
        
        # Neither size nor duration specified
        with pytest.raises(ValueError, match="Either"):
            generate_mock_data(
                seed_file=seed_file,
                output_file=temp_output_file
            )

    def test_high_freq_duration(self, high_freq_seed_file, temp_output_file):
        """Ensure high frequency timestamps are extended correctly by duration"""
        ds_mock = generate_mock_data(
            seed_file=high_freq_seed_file,
            output_file=temp_output_file,
            target_duration_hours=0.001
        )
        ds_seed = xr.open_dataset(high_freq_seed_file)

        ratio_seed = len(ds_seed['high_res_timestamp']) / len(ds_seed['timestamp'])
        ratio_mock = len(ds_mock['high_res_timestamp']) / len(ds_mock['timestamp'])
        assert ratio_mock == ratio_seed

        span = (
            ds_seed['high_res_timestamp'][-1] - ds_seed['high_res_timestamp'][0]
            + (ds_seed['high_res_timestamp'][1] - ds_seed['high_res_timestamp'][0])
        )
        assert ds_mock['high_res_timestamp'][len(ds_seed['high_res_timestamp'])] - ds_mock['high_res_timestamp'][0] == span

    def test_high_freq_size(self, high_freq_seed_file, temp_output_file):
        """Ensure high frequency timestamps are extended correctly by size"""
        ds_seed = xr.open_dataset(high_freq_seed_file)
        target_mb = ds_seed.nbytes / (1024 * 1024) * 3
        ds_mock = generate_mock_data(
            seed_file=high_freq_seed_file,
            output_file=temp_output_file,
            target_size_mb=target_mb
        )

        ratio_seed = len(ds_seed['high_res_timestamp']) / len(ds_seed['timestamp'])
        ratio_mock = len(ds_mock['high_res_timestamp']) / len(ds_mock['timestamp'])
        assert ratio_mock == ratio_seed
    
    def test_large_file_generation(self, seed_file, temp_output_file):
        """Test generating a large file (700MB as mentioned in requirements)"""
        # This test might take a while, so it's marked for optional execution
        ds_mock = generate_mock_data(
            seed_file=seed_file,
            output_file=temp_output_file,
            target_size_mb=700
        )
        
        # Verify the file was created
        assert temp_output_file.exists()
        
        # Check approximate file size
        actual_size_mb = temp_output_file.stat().st_size / (1024 * 1024)
        assert actual_size_mb >= 650  # Allow some tolerance
        
        # Verify data integrity
        assert len(np.unique(ds_mock['timestamp'].values)) == len(ds_mock['timestamp'].values)
        assert len(np.unique(ds_mock['high_res_timestamp'].values)) == len(ds_mock['high_res_timestamp'].values)


if __name__ == "__main__":
    # Run a simple example
    seed_file = Path(__file__).resolve().parent / "data" / "small_data.zarr.zip"
    output_file = Path.cwd() / "mock_data_example.nc"
    
    print("Generating mock data with 100MB target size...")
    ds = generate_mock_data(
        seed_file=seed_file,
        output_file=output_file,
        target_size_mb=100,
        additional_retro_ids=[800, 801]
    )
    
    print(f"Generated dataset with {len(ds['timestamp'])} timestamps")
    print(f"File size: {output_file.stat().st_size / (1024**2):.2f} MB")
    print(f"Duration: {(ds['timestamp'].values[-1] - ds['timestamp'].values[0]) / np.timedelta64(1, 'h'):.2f} hours")
    print(f"Retro IDs: {len(ds['retro'])}")
