
"""
The streaming state machine is designed for efficient and fault-tolerant data upload to cloud storage. 
It operates periodically, such as through a cron job, uploading data in chunks. 

The design emphasizes fault tolerance and recovery from failures to ensure data integrity and continuity.

A screencast on usage and internals of the streaming state machine is available at:
https://miricompany-my.sharepoint.com/personal/ozgun_karagil_mirico_com/_layouts/15/stream.aspx?id=%2Fpersonal%2Fozgun%5Fkaragil%5Fmirico%5Fcom%2FDocuments%2FRecordings%2Fstreaming%20demo%2D20240716%5F120754%2DMeeting%20Recording%2Emp4&nav=eyJyZWZlcnJhbEluZm8iOnsicmVmZXJyYWxBcHAiOiJTdHJlYW1XZWJBcHAiLCJyZWZlcnJhbFZpZXciOiJTaGFyZURpYWxvZy1MaW5rIiwicmVmZXJyYWxBcHBQbGF0Zm9ybSI6IldlYiIsInJlZmVycmFsTW9kZSI6InZpZXcifX0&ct=1722084112697&or=Teams%2DHL&ga=1&referrer=StreamWebApp%2EWeb&referrerScenario=AddressBarCopied%2Eview%2Eea6afd63%2Db602%2D4a96%2Dac65%2D5f374048f7a4

The streaming module builds upon the existing backup feature by introducing the ability to append small chunks of data to an already existing Zarr array on the cloud.

Key Features:
1. **Scheduled Execution**: The service is designed to run periodically, typically every 15-30 minutes, to append new data to the project.
2. **Command Line Execution**: It can be executed manually or scheduled via a cron job. 
3. **Backup Replacement**: This process replaces the previous cloud backup process, eliminating the need for separate backup operations.
4. **Automatic Handling**: The module automatically manages a local state file that tracks uploaded data and handles any failed uploads by deleting incomplete files.
5. **File Organization**: Files are organized by instrument name and project, with high-frequency data stored separately in different Zarr stores.
6. **Retro and Setup Information**: During append operations, new retro and setup information is also appended, ensuring that the project setup information grows as new data is added.
7. **Retro and Setup sideload files**: Project folders include seperate  retro and setup files, that ara guaranteed to have complete setup data, even if setup data was added after creating the project. 
8. **Chunk Management**: Data is appended in chunks, with each variable having its own folder and chunk files. The chunk size is set to 100 timestamps, and 1000 high_freq_timestamps.
9. **Settings Control**: Streaming interval and maximum append duration can be controlled via the command line, API, or settings file.
"""

import yaml
import os
from loguru import logger
from typing import Dict, Union, Tuple, Optional, Any, List
import fsspec
from urllib.parse import urlparse
import xarray as xr
from pathlib import Path
from datetime import datetime, timedelta
import tempfile
from clads import default_settings_filepath
from clads.clads_service.backup import (Backup, 
                                        get_cloud_files, 
                                        append_setup, 
                                        get_missing_setup_ds, 
                                        ensure_fsspec_path, 
                                        setup_sideload_path,
                                        significant_keys,
                                        append_missing_setup_data_to_target)

import pandas as pd
import numpy as np
import shutil
from tenacity import retry, stop_after_attempt, RetryError
import subprocess

zappend_config = {
    'append_dim': 'timestamp',
    'target_dir': '${CLADS_BACKUP_UPLOAD_TARGET}',
    'target_storage_options': {
        'account_name': '${AZURE_STORAGE_ACCOUNT}',
        'sas_token': '${AZURE_STORAGE_SAS_TOKEN}'
    },
   
    # If you are adding anything into 'variables', 'chunking' etc you are doing it wrong!
    # Use 'netcdf_templates.py' and let `zappend` to figure out encoding, chunks 
    # from the data slices. 
    # DON'T DO THIS:
    # 'variables': {
    #     '*': {
    #         'encoding': {
    #             'chunks': None
    #         }
    #     }
    # }
}


default_streaming_settings = {
    'streaming_minutes': 30,
    'streaming_days_per_file': 1,
    # Add other default settings as needed
}


class Streaming:
    """
    Handles the streaming `logic` such as:
        - Discovering the timeframe:  Using system time, last streamed data, local cache and user requested hints.
        - Chunking: original backups process does not aling to chunk borders, and this is a requirement for zappend. 
        - Other zappend operations: Creating zappend configuration, sharing authentication tokens etc. 
        - Appending retro and setup data: Zappend is limited to append on one direction, we need to take care of the setup data which can grow as well. 
        - Seperating high resolution data to seperate files: Once again, Zappend is limited to append on one direction, 
          and we may decide to drop the high resolution data anyway. The streaming process splits the high resolution data to a seperate file.
    """
    def __init__(self, 
                 settings: Union[str, Dict[str, str]],
                 local_root_path: str,
                 target_root: str,
                 since_hint: Optional[pd.Timestamp] = None,
                 until_hint: Optional[pd.Timestamp] = None,
                 keep_files: Optional[str] = None,
                 **storage_options: Dict[str, str]):

        self.backup = Backup(settings)
        self.settings: Dict[str, Any] = self.backup.config.settings

        # Update 'settings' with default values for any keys that are missing
        for key, value in default_streaming_settings.items():
            self.settings.setdefault(key, value)

        # Check if the path exists
        if not os.path.exists(local_root_path):
            # If it does not exist, create it
            os.makedirs(local_root_path, exist_ok=True)

        # make it obvious which path we are using
        local_root_path = os.path.expanduser(local_root_path)
        local_root_path = os.path.abspath(local_root_path)
        logger.info(f"Using {local_root_path} as the local root path")

        self.local_root_path = local_root_path
        self.streamed_paths: List[str] = []  # List of local paths that were successfully streamed

        self.target_root = ensure_upload_path(target_root, **storage_options)
        self.target_url = ''
        self.storage_options = storage_options
        parsed = urlparse(target_root)
        self.fs = fsspec.filesystem(parsed.scheme, auto_mkdir=True, **storage_options)
        self.fs_root_path = parsed.netloc + parsed.path

        self.since = None 
        self.until = None
        self.since_hint = self.convert_to_timestamp(since_hint)
        self.until_hint = self.convert_to_timestamp(until_hint)
        self.last_url : str = ''
        self.ds : Union[xr.Dataset, None] = None
        self.last_ds : Union[xr.Dataset, None] = None
        self.source_ds : Union[xr.Dataset, None] = None
        self.keep_files = keep_files
        self.bytes:int = 0

        yml_path = os.path.join(self.local_root_path, "streaming_state.yaml")
        self.streaming_state = StreamingState(yml_path, target_root, **storage_options)



    def _discover_timeframe(self):

        # if no end date was provided:
        if not self.until_hint:
            self.until_hint = pd.Timestamp.now()

        # `since` is more complicated.  We need to know if we have streamed data before.
        is_available, result = self.streaming_state.initialize_and_validate_paths()
        
        if not is_available:            
            if self.since_hint is None:
                # start from today's date, 00:00:00
                self.since = pd.Timestamp.now().replace(hour=0, minute=0, second=0, microsecond=0)
            else:
                self.since = self.since_hint

            logger.warning(f"Could not find past streamed data")

        else:
            self.last_url, self.last_ds = result
            # previous data is available, but we allow the user to skip forward with the hint.
            # but we should use the hint ONLY if it's for newer data: 
            self.since = self.convert_to_timestamp(self.last_ds.timestamp[-1]) + pd.Timedelta(milliseconds=1)
            if self.since_hint is not None and self.since_hint > self.since:
                self.since = self.since_hint

        self._update_until()               

    def _stream(self):

        with tempfile.TemporaryDirectory() as tmpdirname:

            logger.info(f"Streaming data between {self.since} -- {self.until}")
            
            local_paths = self.backup.to_file(tmpdirname, since=self.since, until=self.until)

            if self.keep_files: 
                # copy from tmpdirname to keep_files
                shutil.copytree(tmpdirname, self.keep_files, dirs_exist_ok=True)
            
            for path in local_paths:

                source_ds = xr.open_dataset(path, engine="zarr") # type: ignore
                
                # Align end to chunk size (unless it's already aligned, as slice (0, -0) ends up empty) 
                if source_ds.timestamp.size % 100 != 0:
                    source_ds = source_ds.isel(timestamp=slice(None, -(source_ds.timestamp.size % 100)))

                if source_ds.timestamp.size == 0:
                    # We have to, and can ignore these: 
                    # - We have to ignore "< chunk" files, as if upload, we can't append to them later, 
                    # - We can ignore them, as if the project data is growing they will be streamed next time. 
                    # - Only risk, is when the project has stopped. The chunk size is small and we can live with
                    #  a few minutes of missing data at the end. 
                    logger.warning(f"Ignoring small file {path}")
                    continue
                else:
                    self.source_ds = source_ds

                # append or create new file? 
                is_appendable = self.is_appendable(self.last_ds, 
                                   self.source_ds, 
                                   significant_keys)
                
                is_within_timeframe = self.is_within_timeframe(self.last_ds, 
                                                               self.source_ds, 
                                                               pd.Timedelta(days=self.settings['streaming_days_per_file'])
                )
                
                if is_appendable and is_within_timeframe:
                    
                    # set the env variable, CLADS_BACKUP_UPLOAD_TARGET for zappend
                    self.target_url = self.last_url
                    os.environ['CLADS_BACKUP_UPLOAD_TARGET'] = self.target_url

                    config = yaml.safe_load(os.path.expandvars(str(zappend_config)))

                    logger.info(f"Appending to {config['target_dir']}")
                    self.streaming_state.on_append_transaction()
                    zappend([path], config, slice_source=self.zappend_append_conform)

                    self.add_high_res()

                    # A good catch, where new setup data appeared in the newer files. 
                    # we make sure the concatenated file has all the setup data.
                    append_missing_setup_data_to_target(self.source_ds, self.last_ds, self.target_url, **self.storage_options)
                    
                else:
                    
                    relative_path = os.path.relpath(path, start=tmpdirname)
                    fs_parent_path = os.path.join(self.fs_root_path, str(Path(relative_path).parent))

                    self.target_url = os.path.join(self.target_root, relative_path)
                    os.environ['CLADS_BACKUP_UPLOAD_TARGET'] = self.target_url
                    config = yaml.safe_load(os.path.expandvars(str(zappend_config)))
                    
                    logger.info(f"Adding new file {config['target_dir']}")
                    self.streaming_state.on_new_transaction(self.target_url)

                    # A Workaround for zappend, as it expects the parent folder to exist.
                    if not self.fs.exists(fs_parent_path):
                        self.fs.touch(os.path.join(fs_parent_path, 'placeholder.txt'))

                    zappend([path], config, slice_source=self.zappend_new_conform)

                    self.add_high_res()
                    
                # continuation of the same settings or not,
                # if it's the same project, the setup data is valid, and extendable, 
                # We continue to append to it.
                self.maintain_project_setup()

                # complete the transaction
                self.last_url, self.last_ds = self.streaming_state.on_complete_transaction()
                self.streamed_paths.append(path)

                # not the most precise way to measure, as high res data is split to a seperate file, 
                # setup side load file etc.. Good enough for now.
                self.bytes += folder_size(path)


    def _progress(self):
        if self.streamed_paths: 
            # we made uploads 
            self.streamed_paths = []
            self.target_url = ''
            self.since = self.convert_to_timestamp(self.source_ds.timestamp[-1]) + pd.Timedelta(milliseconds=1)

        else:
            # we didn't make any uploads, 
            # we should skip to the next day. 
            self.since = self.since + pd.Timedelta(days=self.settings['streaming_days_per_file'])

        self._update_until()

        if self.since >= self.until:
            return False
        else:
            return True
            

    def stream(self):

        start = pd.Timestamp.now()
        self._discover_timeframe()

        while True:
            self._stream()

            if not self._progress():
                break

        total_seconds = (pd.Timestamp.now() - start).total_seconds()
        total_mb = self.bytes / 1024 / 1024
        
        logger.info(f"Streaming completed in {total_seconds:.2f} seconds,"
                    f"{total_mb:.2f} MB uploaded, at "
                    f"{total_mb / total_seconds:.2f} MB/s")

    
    def add_high_res(self): 
        """
        Appends high resolution data to the same folder as the target dataset.  
        """
        target_url = self.target_url.replace('.zarr', '_high_res.zarr')

        drop_dims = set(self.source_ds.dims) - set(['high_res_timestamp'])

        ds = self.source_ds.drop_dims(drop_dims)

        try: 
            logger.info(f"Appending high resolution data to {target_url}")
            ds.to_zarr(target_url, append_dim='high_res_timestamp', mode='a-', storage_options=self.storage_options) # type: ignore
        except Exception as e:
            logger.info(f"Creating new high resolution data at {target_url}")
            ds.to_zarr(target_url, mode='w', storage_options=self.storage_options)



    def maintain_project_setup(self):

        # build the sideload filepath, which should be in the same folder: 
        path = urlparse(self.target_url).path
        setup_url = self.target_url.replace(path, str(Path(path).parent / setup_sideload_path))

        # TODO: There is a risk that this file could get corrupt. It would get recreated but possibly with missing data.
        try: 
            last_setup_ds = xr.open_zarr(setup_url, storage_options=self.storage_options)
            append_missing_setup_data_to_target(self.source_ds, last_setup_ds, setup_url, **self.storage_options)
        except FileNotFoundError:
            drop_dims = set(self.source_ds.dims) - set(['retro', 'settings_id'])
            setup_ds = self.source_ds.drop_dims(drop_dims)
            setup_ds.to_zarr(setup_url, mode='w', storage_options=self.storage_options)
            logger.info(f"Created new setup dataset for the project. {setup_url}")
        except Exception as e:
            logger.warning(f"Could not load setup data for the project, will ignore. {setup_url}: {e}")
        


    def zappend_append_conform(self, path:str) -> xr.Dataset: #
        """
        zappend expects all dimensions other than the append dimension to have the same size/contents
        Changes in settings, retro's etc, can cause these dimensions to grow, and we will handle this
        ourselves, post zappend. For zappend, we  simply make sure the setup information is exactly 
        the same as the target. 
        """

        if self.last_ds is None:
            ds = self.source_ds
        else: 
            setup_dims = set(['retro', 'settings_id'])
            non_setup_dims = set(self.last_ds.dims) - setup_dims

            ds =  self.source_ds.drop_dims(setup_dims, errors='ignore')
            ds = ds.merge(self.last_ds.drop_dims(non_setup_dims))

        ds = ds.drop_dims('high_res_timestamp', errors='ignore')
        return ds
    
    def zappend_new_conform(self, path:str) -> xr.Dataset: #

        ds = self.source_ds.drop_dims('high_res_timestamp', errors='ignore')
        return ds


    def _update_until(self):
        # Where do we stop? 
        # if we are catching up, we try to do a large pieces, at least a day:
        cutoff = self.since.replace(hour=00, minute=00, second=00, microsecond=0) + pd.Timedelta(days=self.settings['streaming_days_per_file'])

        # we add a bit of extra, to guarantee a full chunk.
        cutoff += pd.Timedelta(minutes=self.settings['streaming_minutes'])
        
        # we shouldn't stream past any of these. 
        self.until = min(cutoff, pd.Timestamp.now(), self.until_hint)


    @staticmethod
    def is_appendable(ds1, ds2, significant_keys):
        """
        Use our internal attribute rules to determine if two datasets are appendable.
        This is much more efficient than comparing two files with zappends dry_run=True.        
        """
        if ds1 is None or ds2 is None:
            return False
        
        try: 
            # Check if all significant keys match
            for key in significant_keys:
                if ds1.attrs[key] != ds2.attrs[key]:
                    logger.debug(f"Significant key mismatch: {key}")
                    return False
            return True
        
        except Exception as exc:
            logger.warning(f"Could not confirm dataset compatibility based on significant keys: {exc}")
            return False


    @staticmethod
    def is_within_timeframe(ds1, ds2, max_timedelta):
        """
        Determine if two datasets' timestamps are within a specified timeframe.
        """
        if ds1 is None or ds2 is None:
            return False
        
        try:
            # Check if the timestamps are within max_timedelta        
            timestamp1 = Streaming.convert_to_timestamp(ds1.timestamp[0])
            timestamp1 = timestamp1.replace(hour=0, minute=0, second=0, microsecond=0)
            timestamp1 = timestamp1 + max_timedelta

            timestamp2 = Streaming.convert_to_timestamp(ds2.timestamp[0])

            if timestamp2 > timestamp1:
                logger.debug(f"Duration needs to be broken down: {abs((timestamp2 - timestamp1))}")
                return False
            
            return True
        
        except Exception as exc:
            logger.warning(f"Could not confirm dataset compatibility based on timeframe: {exc}")
            return False

    @staticmethod
    def convert_to_timestamp(time_value):
            # Convert datetime.datetime or numpy.datetime64 to pandas.Timestamp
            
            if time_value is None:
                return None
            
            if isinstance(time_value, (datetime, np.datetime64)):
                return pd.Timestamp(time_value)
            # Handle xarray DataArray with time dimension
            elif isinstance(time_value, xr.DataArray):
                # Assuming 'time' is the name of the time dimension
                return pd.Timestamp(time_value.values)
            else:
                raise ValueError(f"Unsupported time value type {type(time_value)}")
            
            


class StreamingState:
    """
    Manages stateful variables and their relationships for a streaming process, 
    on a local transaction safe, `streaming_state.yaml` file.
    
    This class ensures the integrity and consistency of these variables throughout the lifecycle of the streaming process. 
    Users should interact with these variables through the provided methods to maintain proper state management and 
    avoid direct manipulation of the `streaming_state.yaml` file.
    
    Methods:
        load_state: Loads the streaming state from a YAML file, initializing state variables.
        save_state: Persists the current state to the YAML file, ensuring data consistency.
        on_delete: Should be called once the delete path has been deleted successfully.
        on_begin_transaction: Should be called before attempting to append/update cloud target path. 
        on_complete_transaction: Should be called once the transactions on the target path is complete. 
        on_new_file: Should be called when we are creating a new file on the cloud root path. For example when we start a new day or have to split the day for a settings change. 
    """    

    def __init__(self, state_file_path: str, target_root: str, **storage_options: Dict[str, str]):
        self.state_file_path: str = state_file_path
        self._state_data: Dict[str, str] = self.load_state()
        self.target_root = target_root

        if not self._state_data:  # If the state data is empty (file does not exist or is empty)
            self._state_data = {'last_valid_target': '',        # The last valid target path, guaranteed to be complete.
                                'penultimate_valid_target': '', # The target path before the last valid target.
                                'incomplete_target': ''         # Target path that is being updated, should be deleted if the transaction fails.
            }  # Initialize with default values
            self.save_state()  # Save the state to create the file with default values

        parsed = urlparse(target_root)
        self.storage_options = storage_options
        self.fs = fsspec.filesystem(parsed.scheme, auto_mkdir=True, **storage_options)

    def load_state(self) -> Dict[str, str]:
        """Load the streaming state from the YAML file."""
        try:
            with open(self.state_file_path, 'r') as file:
                return yaml.safe_load(file) or {}
        except FileNotFoundError:
            logger.warning(f"Could not find an existing streaming state file.")            
            return {}  # Return an empty dictionary if the file does not exist
        except yaml.YAMLError as exc:
            logger.warning(f"Error loading the streaming state file: {exc}")
            return {}

    def save_state(self):
        """Save the current state to the YAML file."""
        with open(self.state_file_path, 'w') as file:
            try:
                yaml.safe_dump(self._state_data, file)
            except yaml.YAMLError as exc:
                logger.error(f"Error saving to the streamin state file: {exc}")

    def on_deleted(self): 
        self._state_data['incomplete_target'] = ''
        self.save_state()

    def on_new_transaction(self, file_path:str):
        self._state_data['penultimate_valid_target'] = self._state_data['last_valid_target']
        # no change on,  self._state_data['last_valid_target'], we won't corrupt it. 
        self._state_data['incomplete_target'] = file_path
        self.save_state()

    def on_append_transaction(self):
        self._state_data['incomplete_target'] = self._state_data['last_valid_target']
        self._state_data['last_valid_target'] = self._state_data['penultimate_valid_target']
        self.save_state()

    def on_complete_transaction(self):
        self._state_data['last_valid_target'] = self._state_data['incomplete_target'] 
        self._state_data['incomplete_target'] = ''
        self.save_state()

        ds = load_validate_target_path(self._state_data['last_valid_target'], **self.storage_options)

        return (self._state_data['last_valid_target'], ds)
    
    def initialize_and_validate_paths(self) -> Tuple[bool, Optional[Tuple[str, xr.Dataset]]]:
        """
        Checks the statefull target paths, their existence and validity. 
        If they are not valid, attempts to correct/adjust target paths. 

        If the paths are correct or salvageable, returns the last timestamp in the last_valid_target. 
        If a connection is possible, but paths out of date, or missing, will return default values. 
        
        If the connection or authentication fails, raises an exception, so the process should be cancelled, 
        as we would risk duplication/corruption of streaming backup data. 
        """

        # start with the `incomplete_target`, we can't use xarray here. 
        incomplete_target = self._state_data['incomplete_target']

        if incomplete_target:
            # TODO: How are we holding our urls ? 

            logger.warning(f"Deleting corrupt target: {incomplete_target}")
            try:

                @retry(stop=stop_after_attempt(3))
                def delete_data(incomplete_target:str):
                    parsed = urlparse(incomplete_target)
                    self.fs.rm(parsed.netloc + parsed.path, recursive=True)

                    try: 
                        # we may or may not have high res data, we don't want to cause a fuss if it's missing.
                        high_res_target = parsed.netloc + parsed.path.replace(".zarr", "_high_res.zarr")
                        self.fs.rm(high_res_target, recursive=True)
                    except Exception as exc:
                        logger.warning(f"Could not delete high resolution data: {high_res_target}")
                        logger.debug(exc)

                # Assuming `parsed` is defined elsewhere and passed to this function
                delete_data(incomplete_target)
                
                self.on_deleted()

            # If it's missing we can safely assume it's already deleted. 
            except (FileNotFoundError, RetryError):
                logger.warning(f"Could not find {incomplete_target}, assuming it's already deleted")
                self.on_deleted()
            
            # If it's access rights, failed connection etc, we can't continue. 
            except Exception as exc:
                logger.error(f"Unhandled exception attempting to delete {incomplete_target}: {exc}")
                raise exc


        # we can live without the penultimate target, 
        if self._state_data['penultimate_valid_target'] != '': 
            ds = load_validate_target_path(self._state_data['penultimate_valid_target'], **self.storage_options)

            if ds is None:
                self._state_data['incomplete_target'] = ''
                self.save_state()

        try: 
            ds = load_validate_target_path(self._state_data['last_valid_target'], **self.storage_options)
            
            if ds is None:
                # Could attempt to use the penultimate target, 
                # but can't think of an error condition where the last_valid_target disappears, 
                # and the penultimate one is valid, if `on_append_transaction` is used correctly. 
                
                # instead we look for the last file in the target root path
                # this is an expensive operation.
                logger.warning(f"Could not validate the last valid target {self._state_data['last_valid_target']}")

                # get a list of all the files. 
                logger.warning(f"Attempting to find the last valid target in the target root path: {self.target_root} \n"
                               f"depending on the number of files, this may take a while")    
                
                urls = get_cloud_files(self.target_root, **self.storage_options)
                                    
                files = sorted([Path(urlparse(url).path).parts[-1] for url in urls])

                if len(files):
                    #TODO: If the found file was corrupted at some stage after it was uploaded 
                    # it's going to cause a problem. 
                    last_valid_target = [url for url in urls if files[-1] in url][0]
                    self._state_data['last_valid_target'] = last_valid_target
                    self.save_state()
                    ds = load_validate_target_path(last_valid_target, **self.storage_options)
                else:
                    self._state_data['last_valid_target'] = ''
                    self.save_state()
                    return False, None
            
            logger.info(f"Using last valid target: {self._state_data['last_valid_target']}")
            return True, (self._state_data['last_valid_target'], ds)
            
        except Exception as exc:
            # We can't continue on Authentication, connection or unexpected exceptions.
            # This should exit the state machine
            raise exc



def ensure_upload_path(fsspec_url: str,  **storage_options: Dict[str, str]) -> str:
    parsed = urlparse(fsspec_url)
    fs = fsspec.filesystem(parsed.scheme, auto_mkdir=True, **storage_options)

    if not fs.exists(parsed.netloc + parsed.path):
        fs.makedirs(parsed.netloc + parsed.path)

    return fsspec_url


def load_validate_target_path(fsspec_url: str,  **storage_options: Dict[str, str]) -> Union[xr.Dataset, None]:
    """ Return the target path dataset if it's a valid target path. 
        Return None if the cloud storage is accessible, but the path doesn't exist. 
        raises and exception if the path is not accessible, and can not be validated.
    """

    # if there is no path, it can't be validated, but we continue
    if not fsspec_url: 
        return None
    
    try: 
        ds = xr.open_dataset(fsspec_url, engine="zarr", backend_kwargs=dict(storage_options=storage_options))

        if ds.timestamp.size > 0 or ds.high_res_timestamp.size > 0:
            return ds
        else: 
            logger.warning(f"No usable data found in the target path {fsspec_url}")
            return None
    
    except FileNotFoundError as exc:
        logger.warning(f"Could not find the specified path {fsspec_url}")
        return None
    
    except Exception as exc:
        logger.warning(f"Could not asses the validity of {fsspec_url}")
        raise exc
    


def folder_size(path='.'):
    total = 0
    for entry in os.scandir(path):
        if entry.is_file():
            total += entry.stat().st_size
        elif entry.is_dir():
            total += folder_size(entry.path)
    return total