
import pandas as pd
import os
from pathlib import Path
from tethys_tasks import CaptureNewVariables, running_in_docker, DownloadMonitor, UploadMonitor, CompletenessIndex
from collections.abc import Iterable
import xml.etree.ElementTree as ET
import numpy as np
from meteoraster import MeteoRaster
import importlib.resources as importlib_resources
from tethys_tasks.dropbox_sync import (
    common_dropbox_root,
    compare_local_to_remote_hash,
    delete_dropbox_paths,
    download_file,
    get_dropbox_client,
    list_dropbox_files,
    local_path_to_dropbox_path,
    upload_file,
)
from dropbox.exceptions import AuthError

from azure.storage.blob import BlobServiceClient
from azure.core.credentials import AzureSasCredential
from concurrent.futures import ThreadPoolExecutor, as_completed

class BaseTask():
    '''
    Docstring for BaseTask
    '''

    __DOCKER = running_in_docker()
    if __DOCKER:
        __LOCAL_STORAGE_FOLDER = os.getenv('LOCAL_FILE_FOLDER_DOCKER')
        __TRANSFER_FOLDER = os.getenv('STORAGE_FILE_FOLDER_DOCKER')
    else:
        __LOCAL_STORAGE_FOLDER = os.getenv('LOCAL_FILE_FOLDER')
        __TRANSFER_FOLDER = os.getenv('STORAGE_FILE_FOLDER')

    with CaptureNewVariables() as _BaseTask_VARIABLES: #It is essential that the format of the variable here is _CLASSnAME_VARIABLES
        # All class variables defined below are localized (and eventually superseeded by init kwargs) as _*lowercase* versions,
        # For example, CLEANUP_WINDOW will be accessible as self._cleanup_window.
        AZURE_STORAGE_CONNECTION_STRING = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
        CLOUD_STORAGE_FOLDER = os.getenv('CLOUD_STORAGE_FOLDER')
        DROPBOX_APP_KEY = os.getenv('DROPBOX_APP_KEY', '')
        DROPBOX_APP_SECRET = os.getenv('DROPBOX_APP_SECRET', '')
        DROPBOX_REFRESH_TOKEN = os.getenv('DROPBOX_REFRESH_TOKEN', '')
        DROPBOX_ACCESS_TOKEN = os.getenv('DROPBOX_ACCESS_TOKEN', '')
        DROPBOX_ROOT_PATH = os.getenv('DROPBOX_ROOT_PATH', '/')

        MAX_LOCAL_AGE_MONTHS = os.getenv('MAX_LOCAL_AGE_MONTHS', '')

        ENGINE = 'h5netcdf'

        LOCAL_STORAGE_FOLDER = __LOCAL_STORAGE_FOLDER
        TRANSFER_FOLDER = __TRANSFER_FOLDER

        PUBLICATION_LATENCY = pd.Timedelta(days=0)
        PRODUCTION_FREQUENCY = pd.Timedelta(days=1)
        LEADTIMES = pd.timedelta_range('0d', '0d', freq='1h')
        CLEANUP_WINDOW = pd.DateOffset(months=2)  #### Should be deleted
        CLOUD_PARALLEL_TRANSFERS = int(os.getenv('CLOUD_PARALLEL_TRANSFERS', 3))
        DROPBOX_PARALLEL_TRANSFERS = int(os.getenv('DROPBOX_PARALLEL_TRANSFERS', 3))
        CLOUD_UPLOAD_LOCAL = os.getenv('CLOUD_UPLOAD_LOCAL', False)

        SYNC_LATEST_STORED = os.getenv('SYNC_LATEST_STORED', False)

        SOURCE_PARALLEL_TRANSFERS = 1

        STORAGE_SEARCH_WINDOW = pd.DateOffset(months=14)
        ASSUME_LOCAL_COMPLETE = False

        VARIABLE=''
        SOURCE_KML = ''
        STORAGE_KML = ''
        ZONE = ''
        PIXEL_SIZE = None

        CLOUD_TEMPLATE = f''
        LOCAL_PATH_TEMPLATE = f''
        STORAGE_PATH_TEMPLATE = f''

        DATE_FROM = (pd.Timestamp.utcnow() - pd.Timedelta('7d')).strftime('%Y-%m-%d %H:%M:%S') #'2021-04-15'
        
        FAIL_IF_OLDER = pd.Timedelta('50d')

    def __init__(self, download_from_origin=False, date_from:str='', date_to:str='', verbose=2, *args, **kwargs):
        '''
        Docstring for __init__
        
        kwargs that match class variables (such as CLEANUP_WINDOW) are localized (and eventually superseeded by kwargs) as _*lowercase* versions,
        For example, CLEANUP_WINDOW, cleanup_window, or Cleanup_Window will all be accessible as self._cleanup_window.

        All other kwargs are defined as self. properties without changes.

        !!! bool arguments that may be passed by the docker must be parsed accordingly (see download_from_origin)
        '''

        self.verbose = verbose
        self.diag(f'Initializing...', 1)
        self.diag(f'    Running from docker: {self.__DOCKER}', 1)

        self._set_base_variables(BaseTask, kwargs)
        self.blob_service_client = None
        self.container_client = None
        self.dropbox_client = None

        self._variable_upper = self._variable.upper()
        self._variable_lower = self._variable.lower()
        self._cloud_template = self._cloud_template.format(self=self)
        self._local_path_template = self._local_path_template.format(self=self)
        self._storage_path_template = self._storage_path_template.format(self=self)

        self._download_from_origin = download_from_origin
        if isinstance(self._download_from_origin, str):
            self._download_from_origin = self._download_from_origin=='True'

        self.source_bounding_box = None
        if self._source_kml.endswith('.kml'):
            self.source_bounding_box = self._get_bounding_box(self._source_kml, self._pixel_size)
        else:
            self.source_bounding_box = dict(north=90, west=-180, south=-90, east=180)
        self.storage_bounding_box = None
        if self._storage_kml.endswith('.kml'):
            self.storage_bounding_box = self._get_bounding_box(self._storage_kml, self._pixel_size)

        self.last_production_datetime = pd.Timestamp.now() - self._publication_latency
        self.data_index = self.populate(date_from, date_to)
        
    def _get_cloud_connection(self):
        '''
        Create a connection to Azure if it does not exist and returns it
        '''

        if self.blob_service_client is None or self.container_client is None:
            full_url, sas_token = self._azure_storage_connection_string.split('?', 1)
            account_url = '/'.join(full_url.split('/')[:-1])
            container_name = full_url.split('/')[-1]
            self.blob_service_client = BlobServiceClient(account_url=account_url, credential=AzureSasCredential(sas_token))
            self.container_client = self.blob_service_client.get_container_client(container_name)

        return (self.blob_service_client, self.container_client)

    def _get_dropbox_connection(self):
        '''
        Create a connection to Dropbox if it does not exist and returns it
        '''

        if self.dropbox_client is None:
            try:
                self.dropbox_client = get_dropbox_client(
                    refresh_token=self._dropbox_refresh_token,
                    app_key=self._dropbox_app_key,
                    app_secret=self._dropbox_app_secret,
                    access_token=self._dropbox_access_token,
                )
                self.dropbox_client.check_user('tethys-tasks')
            except AuthError as exc:
                raise RuntimeError(
                    'Dropbox authentication failed. If you copied the token shown as "Generated access token" '
                    'from the App Console, store it in DROPBOX_ACCESS_TOKEN and leave DROPBOX_REFRESH_TOKEN empty. '
                    'For unattended sync, obtain a real OAuth refresh token and store it in DROPBOX_REFRESH_TOKEN.'
                ) from exc

        return self.dropbox_client

    def _set_base_variables(self, cls, kwargs):
        '''
        Docstring for __set_base_variables
        
        Returns a list of arguments that were not parsed
        '''

        classes = [cls.__name__ for cls in self.__class__.__mro__ if cls not in [object]][::-1]

        to_ignore = []
        for cls in classes:
            variable_dict = getattr(self, f'_{cls}_VARIABLES').new_vars
            # del variable_dict[f'_{cls}_VARIABLES']
            to_ignore.append(f'_{cls}_VARIABLES')

            default_vars = {k.upper(): v for k, v in variable_dict.items() if k not in to_ignore}

            for k, v in kwargs.items():
                upper = k.upper()
                lower = k.lower()
                if upper in default_vars:
                    _ = default_vars.pop(upper)
                    setattr(self, f'_{lower}', v)
                else:
                    setattr(self, k, v)
            
            for k, v in default_vars.items():
                lower = k.lower()
                setattr(self, f'_{lower}', v)

    def populate(self, date_from:str='', date_to:str='', silent:bool=False, additional_columns:dict={}) -> pd.DataFrame:
        '''
        Returns a dataframe that serves as index to the data retrieval and pre-processing tasks.

        :param date_from: Production datetime of the first file to be considered. Specified as a string '%Y-%m-%d', '%Y-%m-%d %H:%M:%S', etc. If left empty, the default is used.
        :param date_to: Production datetime of the last file to be considered. Specified as a string '%Y-%m-%d', '%Y-%m-%d %H:%M:%S', etc. If left empty, the present is used.
        
        :param silent: Do not generate prints
        :param additional_columns: dict with {column_header: lamdba function, ...} that is applied along the columns of the data_index

        :return: the returned dataframe must have the following columns:
            production_datetime (pd.Timestamp)
            leadtime (pd.Timedelta or pd.DateOffset)
            event_datetime (pd.Timestamp)
            cloud_file (str)
            local_file (str)
            stored_file (str)
            dropbox_file (str)
            cloud_file_exists (bool)
            local_file_exists (bool)
            stored_file_exists (bool)
            data_exists (bool)
            local_file_complete (bool)
            stored_file_complete (bool)
            idx (int)
        '''

        if not silent:
            self.diag(f'    Populating...', 1)
            self.diag(f'        Adapting dates.', 2)
        reference_past = '1900-01-01'

        if date_from == '':
            date_from = self._date_from
        if date_to=='':
            date_to = self.last_production_datetime.isoformat()

        date_from = pd.date_range(reference_past, date_from, freq=self._production_frequency)[-1]
        production_datetimes = pd.date_range(date_from, date_to, freq=self._production_frequency)
        leadtimes = self._leadtimes

        if not silent:
            self.diag(f'        Creating index.', 2)
        # This operation can be costly
        index = pd.MultiIndex.from_product([production_datetimes, leadtimes], names=['production_datetime', 'leadtime']).to_frame(index=False)

        # Handle event_datetime separately for Timedelta vs DateOffset to ensure vectorized operations
        if len(index) > 0 and isinstance(index['leadtime'].iloc[-1], pd.Timedelta):
            event_datetime = index['production_datetime'] + index['leadtime']
        else:
            event_datetime = index.apply(lambda row: row['production_datetime'] + row['leadtime'], axis=1)
        
        index = index.assign(
            event_datetime=event_datetime,
            doy=index.production_datetime.dt.day_of_year,
        )

        if isinstance(index['leadtime'][0], pd.Timedelta):
            index = index.assign(
                lt_days=(index['leadtime'] / pd.Timedelta(days=1)).astype('int64'),
                lt_hours=(index['leadtime'] / pd.Timedelta(hours=1)).astype('int64'),
            )
        elif isinstance(index['leadtime'][0], pd.DateOffset):
            #created as this [pd.DateOffset(months=i) for i in range(7)] (always defined in months for simplicity)
            if hasattr(index['leadtime'].iloc[-1], 'months'):
                index = index.assign(lt_years=index['leadtime'].apply(lambda x: x.months))
            else:
                index = index.assign(lt_years=index['leadtime'].apply(lambda x: x.years))

        for k0, f0 in additional_columns.items():
            index.loc[:, k0] = f0(index)
            
        if not silent:
            self.diag(f'        Parsing cloud paths.', 2)
        cloud_folder = self._cloud_storage_folder
        cloud_template = self._cloud_template
        index['cloud_file'] = index['production_datetime'].dt.strftime(cloud_template)
        if '{' in cloud_template:
            index['cloud_file'] = index.apply(lambda x: x['cloud_file'].format(**x.to_dict()), axis=1)
        index['cloud_file'] = cloud_folder + '/' + index['cloud_file']

        if not silent:
            self.diag(f'        Parsing local paths.', 2)
        local_storage_folder = self._local_storage_folder
        local_path_template = self._local_path_template
        index['local_file'] = index['production_datetime'].dt.strftime(local_path_template)
        if '{' in local_path_template:
            index['local_file'] = index.apply(lambda x: x['local_file'].format(**x.to_dict()), axis=1)
        index['local_file'] = local_storage_folder + '/' + index['local_file']
        
        if not silent:
            self.diag(f'        Parsing storage paths.', 2)
        transfer_folder = self._transfer_folder
        storage_path_template = self._storage_path_template
        index['stored_file'] = index['production_datetime'].dt.strftime(storage_path_template)
        if '{' in storage_path_template:
            index['stored_file'] = index.apply(lambda x: x['stored_file'].format(**x.to_dict()), axis=1)
        index['stored_file'] = transfer_folder + '/' + index['stored_file']

        index['dropbox_file'] = index['stored_file'].apply(lambda x: local_path_to_dropbox_path(x, self._transfer_folder, self._dropbox_root_path))

        index = index.assign(
            cloud_file_exists=False,
            local_file_exists=False,
            stored_file_exists=False,
            data_exists=False,
            local_file_complete=False,
            stored_file_complete=False,
            idx = index.index
        )

        index.index.name = 'idx'

        return index

    def retrieve(self, fail_if_older:bool=False, *args, **kwargs) -> bool:
        '''
        Docstring for retrieve
        
        :param file_path: Description
        '''

        self.diag('Retrieving...', 1)

        self._update_index_and_completeness()

        downloaded = self._download_from_cloud()

        if self._download_from_origin:
            downloaded = self._download_from_source()
            if downloaded:
                self._update_index_and_completeness(stored=False, cloud=False)
        else:
            self.diag('    Retrieval from origin skipped due to class kwargs.', 1)

        self.complete_local_files()

        self._update_index_and_completeness(stored=False, cloud=False)

        self.diag('    Done retrieving.', 1)

        if fail_if_older:
            self._check_cutoff()

        return downloaded

    def _check_cutoff(self, fail_if_older:bool=True) -> bool:
        '''
        Checks if data exists and is recent enough based on a predefined cutoff period.
        Args:
            fail_if_older (bool): If True, raises an exception if data is missing or outdated.
        Returns:
            bool: True if data exists and is within the cutoff period, False otherwise.
        Raises:
            Exception: If verify_new_data is True and no data exists or data is older than the cutoff.
        '''
        
        success = True

        data_exists = self.data_index[['production_datetime', 'data_exists']].groupby('production_datetime').all()
        data_exists = data_exists.loc[data_exists.values].index
        if data_exists.shape[0]==0:
            success = False
            if fail_if_older:
                raise Exception(f'No data exists for the period ({self.__class__.__name__}).')
        else:
            last_date = data_exists[-1]
            cutoff_date = pd.Timestamp.utcnow().tz_localize(None) - self._fail_if_older
            if last_date<=cutoff_date:
                success = False
                if fail_if_older:
                    raise Exception(f'No recent data exists ({self.__class__.__name__}). Last production: {last_date.strftime("%Y-%m-%d %H:%M:%S")}.')

        return success

    def acquisition_status(self, refresh:bool=False) -> dict:
        '''
        Reports the date of the last successful data acquisition and the success
        rate (fraction of leadtimes hit) at that date.

        :param refresh: When False (default) the current state of self.data_index
            is used as-is, so run retrieve()/update() beforehand for up-to-date
            results. When True, the index is first refreshed from stored and local
            files (cloud=False) via _update_index_and_completeness, which is fast
            and network-free (it relies on the completeness.csv sidecars). Use
            refresh=True for a standalone report (e.g. an Airflow report DAG) that
            has not just run a retrieval in the same process.

        :return: dict with keys
            last_acquisition (pd.Timestamp | None): most recent production_datetime
                with at least one leadtime hit. None if no data exists.
            success_rate (float | None): hit_leadtimes / total_leadtimes at that
                date (0..1). None if no data exists.
            hit_leadtimes (int): leadtimes with data at that date.
            total_leadtimes (int): leadtimes indexed for that date.
        '''

        if refresh:
            self._update_index_and_completeness(stored=True, local=True, cloud=False)

        result = dict(last_acquisition=None, success_rate=None,
                      hit_leadtimes=0, total_leadtimes=0)

        hits = self.data_index.loc[self.data_index['data_exists'], 'production_datetime']
        if hits.empty:
            return result

        last_acquisition = hits.max()
        at_last = self.data_index.loc[self.data_index['production_datetime'] == last_acquisition]
        total = int(len(at_last))
        hit = int(at_last['data_exists'].sum())

        result.update(
            last_acquisition=last_acquisition,
            success_rate=(hit / total) if total else None,
            hit_leadtimes=hit,
            total_leadtimes=total,
        )
        return result

    def _check_existing_files(self, stored:bool=True, local:bool=True, cloud:bool=True) -> None:
        '''
        Updates the data_index dataframe.

        Checks what files exist in storage, locally, or in the cloud, in that order.
        Once found in a "higher" order, the data is not looked for in lower orders.
            For example, data already in storage will not be subject to search locally or in the cloud.

        False may appear even if files do exist, if they have not been checked (stored -> local -> cloud).

        The updated dataframe will have the following fields updated:
            stored_file_exists
            local_file_exists
            cloud_file_exists

            local_file_complete (partial update, only in the case of complete files)
            stored_file_complete (partial update, only in the case of complete files)
        '''

        self.data_index.loc[:, 'local_file_complete'] = False
        self.data_index.loc[:, 'stored_file_complete'] = False
        self.data_index.loc[:, 'data_exists'] = False

        # Stored files
        if stored:
            stored_files = self.data_index['stored_file'].unique()
            self.data_index.loc[:, 'stored_file_exists'] = False
            
            # Build set of existing files using rglob
            if len(stored_files) > 0:
                extension = Path(stored_files[0]).suffix
                search_roots = set(Path(f).parents[2] for f in stored_files if len(Path(f).parents) > 2)
                existing_files = set()
                for root in search_roots:
                    if root.exists():
                        existing_files.update(p for p in root.rglob(f'*{extension}'))
                
                # Batch update using vectorized isin()
                mask = pd.Series([Path(p) for p in self.data_index['stored_file']], index=self.data_index.index).isin(existing_files)
                self.data_index.loc[mask, 'stored_file_exists'] = True

            for folder in set([Path(f).parent for f in stored_files]):
                ci = CompletenessIndex(folder)
                for name in ci.get_complete():
                    for s0 in stored_files:
                        stored_file = None
                        if s0.endswith(name):
                            stored_file = s0
                            break
                    if stored_file:
                        self.data_index.loc[self.data_index['stored_file']==stored_file, 'stored_file_complete'] = True

        # Local files
        if local:
            local_files = self.data_index['local_file'].unique()
            self.data_index.loc[:, 'local_file_exists'] = False
            
            # Build set of existing files using rglob
            if len(local_files) > 0:
                extension = Path(local_files[0]).suffix
                search_roots = set(Path(f).parents[2] for f in local_files if len(Path(f).parents) > 2)
                existing_files = set()
                for root in search_roots:
                    if root.exists():
                        existing_files.update(p for p in root.rglob(f'*{extension}'))
                
                # Batch update using vectorized isin()
                mask = pd.Series([Path(p) for p in self.data_index['local_file']], index=self.data_index.index).isin(existing_files)
                self.data_index.loc[mask, 'local_file_exists'] = True

            if len(local_files) > 0:
                local_file_map = {Path(l0).name: l0 for l0 in local_files}
                for folder in set([Path(f).parent for f in local_files]):
                    ci = CompletenessIndex(folder)
                    complete_names = ci.get_complete()
                    if not complete_names:
                        continue
                    complete_files = [local_file_map[n] for n in complete_names if n in local_file_map]
                    if complete_files:
                        mask = self.data_index['local_file'].isin(complete_files)
                        self.data_index.loc[mask, 'local_file_complete'] = True

        # Cloud files
        if cloud:
            cloud_files = self.data_index.loc[~self.data_index['stored_file_complete'] & ~self.data_index['local_file_complete'], 'cloud_file'].unique()
            self.data_index.loc[:, 'cloud_file_exists'] = False
            cloud_files_exist = self._check_cloud(cloud_files)
            
            # Batch update using vectorized operation
            existing_cloud_files = [f for f, exists in zip(cloud_files, cloud_files_exist) if exists]
            if existing_cloud_files:
                mask = self.data_index['cloud_file'].isin(existing_cloud_files)
                self.data_index.loc[mask, 'cloud_file_exists'] = True

    def _load_stored_file(self, stored_file:str):
        '''
        Loads a stored file, returning None when it cannot be read.

        A file truncated by an unclean shutdown is unreadable by HDF5 and used to abort
        the whole run. Callers treat None as "not stored yet", so the file is rebuilt
        from the local files (or re-downloaded from Dropbox) instead.
        '''

        try:
            return MeteoRaster.load(stored_file, verbose=False)
        except Exception as ex:
            print(f'        Stored file unreadable, it will be rebuilt: {stored_file} ({ex}).')
            return None

    def _discard_unreadable_stored_file(self, stored_file:str, idx, ex:Exception=None) -> None:
        '''
        Marks an unreadable stored file as missing so it is regenerated downstream.
        '''

        if ex is not None:
            print(f'        Stored file unreadable, treating it as missing: {stored_file} ({ex}).')

        self.data_index.loc[idx, ['stored_file_exists', 'stored_file_complete', 'data_exists']] = False

    def _check_existing_data(self, stored:bool=True, local:bool=True, cloud:bool=True, **kwargs) -> None:
        '''
        Checks data one file at a time
        Updates an index file that marks complete files when there are changes (per folder)
        '''

        self._check_existing_files(stored=stored, local=local, cloud=cloud, **kwargs)


        self.data_index.set_index(['production_datetime', 'leadtime'], append=False, drop=False, inplace=True)

        previous_files = self.__get_files_by_production_datetime(self.data_index['production_datetime'].min()-self._production_frequency)
        posterior_files = self.__get_files_by_production_datetime(self.data_index['production_datetime'].max()+self._production_frequency)

        # Storage files
        if stored:
            stored_files = self.data_index.loc[self.data_index['stored_file_exists'], 'stored_file'].unique()
            for s0 in stored_files[::-1]:
                # Trusts completeness if marked as complete, otherwise checks it.
                idx = self.data_index['stored_file']==s0
                idx = idx.loc[idx].index
                if self.data_index.loc[idx, 'stored_file_complete'].all():
                    self.data_index.loc[idx, 'data_exists'] = True
                    continue

                try:
                    stored_complete = MeteoRaster.get_completeness(s0)
                except Exception as ex:
                    self._discard_unreadable_stored_file(s0, idx, ex)
                    continue

                if stored_complete:
                    self.data_index.loc[idx, 'data_exists'] = True
                    self.data_index.loc[idx, 'stored_file_complete'] = True
                else:
                    self.data_index.loc[idx, 'stored_file_complete'] = False

                    data = self._load_stored_file(s0)
                    if data is None:
                        self._discard_unreadable_stored_file(s0, idx)
                        continue
                    complete_index = data.get_complete_index().stack()

                    idx = complete_index.index.isin(self.data_index.index)
                    complete_index = complete_index.loc[idx, :]

                    self.data_index.loc[complete_index.index, 'data_exists'] = complete_index | self.data_index.loc[complete_index.index, 'data_exists']

        # Local files
        if local:
            # Based on completeness files and existence (if ASSUME_LOCAL_COMPLETE is True)
            mask = self.data_index['local_file_complete']
            if self._assume_local_complete:
                mask |= self.data_index['local_file_exists']        
            self.data_index.loc[mask, ['data_exists', 'local_file_complete']] = True
            
            # Retrieves all local files in the index that are not marked as complete, but exist (either marked as existing or not checked yet)
            local_files = self.data_index.loc[~self.data_index['local_file_complete'] & self.data_index['local_file_exists'], 'local_file'].unique()

            # Checks individual files not marked as complete
            for l0 in local_files[::-1]:
                idx = self.data_index['local_file']==l0
                idx = idx.loc[idx].index
                
                valid_steps = self.read_local_completeness(l0)

                self.data_index.loc[valid_steps.index, 'data_exists'] = valid_steps | self.data_index.loc[valid_steps.index, 'data_exists']

                complete_index = self.data_index.loc[idx, 'local_file_complete']
                complete_index.loc[valid_steps.index] = True
                self.data_index.loc[idx, 'local_file_complete'] = complete_index.all()

            # Fix completeness at the edges (maybe False when complete, but the check has not been made)
            self.data_index.loc[self.data_index['local_file']==previous_files['local_file'], 'local_file_complete'] = False
            self.data_index.loc[self.data_index['local_file']==posterior_files['local_file'], 'local_file_complete'] = False

        self.data_index.set_index(['idx'], append=False, drop=False, inplace=True)

    def read_local_completeness(self, local_file:str) -> pd.DataFrame:
        '''
        Returns a pd.Series with the valid steps of a local file
        [production_datetime  leadtime] [Bool]

        Can be overloaded when a full read is not necessary
        '''

        data = self.read_local(local_file)
        axes = (1, 3, 4)
        data_steps = pd.DataFrame(np.sum(np.isfinite(data.data), axis=axes)>0,
                                index=pd.DatetimeIndex(data.production_datetime, name='production_datetime'),
                                columns=pd.Index(data.leadtimes, name='leadtime')).stack()
        
        valid_steps = data_steps[data_steps]
        valid_steps = valid_steps.loc[valid_steps.index.isin(self.data_index.index)]

        return valid_steps

    def __get_files_by_production_datetime(self, production_datetime:pd.Timestamp) -> dict:
        '''
        returns a self evident dict with the cloud, local, and stored file paths corresponding to a given production_datetime
        produces and error if the provided datetime is not aligned
        uses self.populate to do this, minimizing redudancy
        '''

        index = self.populate(production_datetime, production_datetime, silent=True).loc[0]

        if index['production_datetime']!=production_datetime:
            raise Exception(f'The provided production datetime ({production_datetime}) is not aligned ({self.__class__.__name__}).')

        return dict(cloud_file=index['cloud_file'],
                    local_file=index['local_file'],
                    stored_file=index['stored_file'],
                    )

    def _update_completeness(self, stored:bool=True, local:bool=True) -> None:
        '''
        Docstring for _update_completeness
        
        :param self: Description
        :param stored: Description
        :type stored: bool
        :param local: Description
        :type local: bool
        '''

        if stored:
            stored_files = self.data_index[['stored_file', 'stored_file_complete']].groupby('stored_file').all()
            stored_files.loc[:, 'folder'] = [Path(f).parent for f in stored_files.index]
            stored_files = stored_files.reset_index().set_index(['folder', 'stored_file'])
            for folder in stored_files.index.get_level_values('folder').unique():
                ci = CompletenessIndex(folder)
                to_remove = []
                to_include = []
                for stored_file, complete in stored_files.loc[folder, :].iterrows():
                    stored_file_ = Path(stored_file)
                    if complete.iloc[0]:
                        to_include.append(stored_file_.name)
                    else:
                        to_remove.append(stored_file_.name)
                ci.include(to_include)
                ci.remove(to_remove)
                ci.write()

        if local:
            local_files = self.data_index[['local_file', 'local_file_complete']].groupby('local_file').all()
            local_files.loc[:, 'folder'] = [Path(f).parent for f in local_files.index]
            local_files = local_files.reset_index().set_index(['folder', 'local_file'])
            for folder in local_files.index.get_level_values('folder').unique():
                ci = CompletenessIndex(folder)
                to_remove = []
                to_include = []
                for local_file, complete in local_files.loc[folder, :].iterrows():
                    local_file_ = Path(local_file)
                    if complete.iloc[0]:
                        to_include.append(local_file_.name)
                    else:
                        to_remove.append(local_file_.name)
                ci.include(to_include)
                ci.remove(to_remove)
                ci.write()

    def _update_index_and_completeness(self, stored:bool=True, local:bool=True, cloud:bool=True) -> None:
        '''
        Checks files and data
        Update the completeness files
        '''

        self._check_existing_data(stored=stored, local=local, cloud=cloud)
        self._update_completeness(stored=stored, local=local)

    def _check_cloud(self, azure_paths: Iterable):
        '''
        Returns a boolean list
        '''
        
        cloud_paths = pd.Series(azure_paths).str.split('/', expand=True)
        root_paths = []
        if cloud_paths.shape[0]==0:
            return [False] * len(azure_paths)
        elif cloud_paths.shape[0]==1:
            # Only one file - direct search
            root_paths.append('/'.join(cloud_paths.iloc[0,:].to_list()))
        else:
            # More than one file - several containers at a time (to minimize azure hits and costs)
            # Looks up for common roots among all files
            i0 = 0
            while i0<cloud_paths.shape[1] and len(cloud_paths.iloc[:, i0].unique())==1:
                i0 += 1
            i0 = min((max((3, i0)), cloud_paths.shape[1]-1))
            for _, i1 in cloud_paths.iloc[:, :i0].drop_duplicates().iterrows():
                root_paths.append('/'.join(i1.to_list()))
                
        # Create BlobServiceClient
        blob_service_client, container_client = self._get_cloud_connection()
        def list_blobs(cloud_path: str) -> set:
            blobs = set()
            try:
                for blob in container_client.list_blobs(name_starts_with=cloud_path):
                    blobs.add(blob.name)
            except Exception as e:
                print(f"Error listing blobs for prefix '{cloud_path}': {e}")
            return blobs

        returned_paths = set()
        with ThreadPoolExecutor(max_workers=self._cloud_parallel_transfers) as executor:
            futures = [executor.submit(list_blobs, cloud_path) for cloud_path in root_paths]
            for future in as_completed(futures):
                returned_paths.update(future.result())

        return [ap in returned_paths for ap in azure_paths]

    def clear_completeness_files(self, local:bool=True, stored:bool=True) -> None:
        '''
        Clears completeness files in the local and storage folders, if they exist.
        '''

        if local:
            local_folders = self.data_index['local_file'].apply(lambda x: Path(x).parent).unique()
            for folder in local_folders:
                ci = CompletenessIndex(folder)
                ci.erase()

            print('Completeness files cleared.')

        if stored:
            stored_folders = self.data_index['stored_file'].apply(lambda x: Path(x).parent).unique()
            for folder in stored_folders:
                ci = CompletenessIndex(folder)
                ci.erase()

        self.diag(f'    Completeness files cleared ({self.__class__.__name__}).', 1)

    @staticmethod
    def _get_bounding_box(kml_file:str, pixel:float) -> dict:
        '''
        Gets a rectangular bounding box cooresponding to the geometry kml file.
        Increases the bounding box to the outer pixel edge.

        Returns a self evident dictionary
        '''

    
        def mroundup(x, multiple):
            return np.round(np.ceil(x / multiple) * multiple, 6)
        
        def mrounddown(x, multiple):
            return np.round(np.floor(x / multiple) * multiple, 6)
        
        kml_path = Path(kml_file)
        if kml_path.exists():
            tree = ET.parse(kml_path)
            root = tree.getroot()
        else:
            resource_name = kml_path.name
            try:
                with importlib_resources.open_text("tethys_tasks.resources", resource_name) as handle:
                    root = ET.fromstring(handle.read())
            except FileNotFoundError as exc:
                raise FileNotFoundError(f'KML file not found: {kml_file}') from exc
        ns = {'kml': 'http://www.opengis.net/kml/2.2'}

        latitudes = []
        longitudes = []

        for coord in root.findall('.//kml:coordinates', ns):
            if coord.text:
                coords_text = coord.text.strip()
                coords_list = coords_text.replace('\n', ' ').split()
                for item in coords_list:
                    parts = item.strip().split(',')
                    if len(parts) >= 2:
                        lon, lat = float(parts[0]), float(parts[1])
                        longitudes.append(lon)
                        latitudes.append(lat)

        if not latitudes or not longitudes:
            return None  # No valid coordinates found

        return dict(north=mroundup(max(latitudes), pixel),
                    west=mrounddown(min(longitudes), pixel),
                    south=mrounddown(min(latitudes), pixel),
                    east=mroundup(max(longitudes), pixel))

    def _download_from_cloud(self) -> bool:
        '''
        Updates local files
        '''

        self.diag(f'    Downloading from Azure...', 1)
        downloaded = False

        to_download = self.data_index.loc[self.data_index['cloud_file_exists'] & ~self.data_index['local_file_exists'], ['local_file', 'cloud_file']].drop_duplicates()

        if to_download.empty:
            self.diag(f'        Nothing to download.', 2)
            return False

        blob_service_client, container_client = self._get_cloud_connection()

        def download_row(local_path: str, blob_path: str) -> bool:
            path = Path(local_path)
            try:
                path.parent.mkdir(parents=True, exist_ok=True)
                blob_client = container_client.get_blob_client(blob_path)
                with open(path, 'wb') as handle:
                    handle.write(blob_client.download_blob().readall())
                return True
            except Exception as ex:
                print(f'        Error downloading {blob_path} -> {path.absolute()}: {ex}.')
                return False

        with DownloadMonitor() as monitor:
            with ThreadPoolExecutor(max_workers=self._cloud_parallel_transfers) as executor:
                futures = {executor.submit(download_row, r['local_file'], r['cloud_file']): (r['local_file'], r['cloud_file']) for _, r in to_download.iterrows()}
                for future in as_completed(futures):
                    success = future.result()
                    if success:
                        local_file, cloud_file = futures[future]
                        self.data_index.loc[self.data_index['local_file']==local_file, ['local_file_exists', 'local_file_complete' , 'data_exists']] = True
                        downloaded = True

                        msg = monitor.mark_success(local_file)
                        self.diag('        ' + msg, 1)

        if downloaded:
            self._check_existing_data(stored=False, cloud=False)

        return downloaded

    def _clean_index(self) -> None:
        '''
        Cleans the "variable" parts of self.data_index by setting them as False
        '''

        self.data_index.loc[:, ['cloud_file_exists', 'local_file_exists', 'stored_file_exists', 'data_exists', 'local_file_complete', 'stored_file_complete']] = False

        self.data_index.index = pd.Index(np.arange(self.data_index.shape[0]), name='idx')
        self.data_index.loc[:, 'idx'] = self.data_index.index

    def _upload_to_cloud(self) -> bool:
        '''
        Docstring for _upload_to_cloud
        
        :param self: Description
        :param args: Description
        :param kwargs: Description
        '''

        uploaded = False

        if not self._cloud_upload_local:
            self.diag('    Nothing will be uploaded.', 1)
            return uploaded

        self.diag('    Uploading to Azure...', 1)

        self.diag('        Building index...', 2)
        self._clean_index()

        self._update_index_and_completeness(stored=False, cloud=False)

        to_upload = self.data_index.loc[self.data_index['local_file_complete'], ['local_file', 'cloud_file']].drop_duplicates()
        if to_upload.empty:
            self.diag('        Nothing to upload.', 2)
            return False

        # Check cloud files
        self.diag('        Querying cloud...', 2)
        existing_cloud = self._check_cloud(to_upload['cloud_file'])
        to_upload = to_upload.loc[[not exists for exists in existing_cloud]]
        if to_upload.empty:
            self.diag('        Nothing to upload.', 2)
            return False

        blob_service_client, container_client = self._get_cloud_connection()

        # Upload
        self.diag(f'        Uploading ({self._cloud_parallel_transfers} threads)...', 2)
        def upload_row(local_path: str, blob_path: str) -> bool:
            try:
                path = Path(local_path)
                with path.open('rb') as handle:
                    container_client.get_blob_client(blob_path).upload_blob(handle, overwrite=True)
                return True
            except Exception as ex:
                print(f'        Error uploading {local_path} -> {blob_path}: {ex}.')
                return False

        with UploadMonitor() as monitor:
            with ThreadPoolExecutor(max_workers=self._cloud_parallel_transfers) as executor:
                futures = {executor.submit(upload_row, r['local_file'], r['cloud_file']): (r['local_file'], r['cloud_file']) for _, r in to_upload.iterrows()}
                for future in as_completed(futures):
                    success = future.result()
                    if success:
                        local_file, cloud_file = futures[future]
                        self.data_index.loc[self.data_index['cloud_file']==cloud_file, 'cloud_file_exists'] = True
                        uploaded = True
                        
                        msg = monitor.mark_success(local_file, cloud_file)
                        self.diag('        ' + msg, 1)

        return uploaded

    def _sync_latest_stored_download(self) -> bool:
        '''
        Syncs stored files from dropbox
        Downloads the existing files.
        Overwrites the ones that do not have the same hash
        The storage path is based on the stored file path
        '''

        sync = False

        if not self._sync_latest_stored:
            self.diag('    No stored files will be synced.', 1)
            return sync

        self.diag('    Syncing latest stored files from Dropbox...', 1)

        self._update_index_and_completeness(stored=True, local=False, cloud=False)

        client = self._get_dropbox_connection()
        dropbox_root_path = common_dropbox_root(self.data_index['dropbox_file'].unique(), self._dropbox_root_path)

        remote_files = list_dropbox_files(client, dropbox_root_path)
        if not remote_files:
            self.diag('        Nothing to download.', 2)
            return sync

        to_download = []
        db_2_stored = self.data_index.groupby('dropbox_file').first().loc[:, ['stored_file']]
        for remote_metadata in remote_files.values():
            local_path = db_2_stored.loc[remote_metadata['path_display'], 'stored_file'] if remote_metadata['path_display'] in db_2_stored.index else None
            if not local_path:
                continue

            local_file = Path(local_path)
            if not local_file.exists():
                to_download.append((remote_metadata, local_path))
                continue

            if local_file.stat().st_size < remote_metadata['size']:
                to_download.append((remote_metadata, local_path))

        if not to_download:
            self.diag('        Dropbox files already match local storage.', 2)
            return sync

        self.diag(f'        Downloading ({self._dropbox_parallel_transfers} threads)...', 2)

        with DownloadMonitor() as monitor:
            with ThreadPoolExecutor(max_workers=self._dropbox_parallel_transfers) as executor:
                futures = {
                    executor.submit(download_file, client, remote_metadata['path_display'], local_path): (remote_metadata, local_path)
                    for remote_metadata, local_path in to_download
                }
                for future in as_completed(futures):
                    metadata, local_path = futures[future]
                    try:
                        future.result()
                    except Exception as ex:
                        print(f'        Error downloading {metadata["path_display"]} -> {local_path}: {ex}.')
                        continue

                    sync = True
                    self.diag('        ' + monitor.mark_success(str(local_path)), 1)

        if sync:
            self._update_index_and_completeness(stored=True, local=False, cloud=False)

        return sync

    def _sync_latest_stored_upload(self) -> bool:
        '''
        Syncs stored files to dropbox
        Uploads the latest three stored files.
        Deletes all the older ones.
        Overwrites the ones that do not have the same hash
        The storage path is based on the stored file path
        '''
    
        sync = False

        if not self._sync_latest_stored:
            self.diag('    Nothing will be synced.', 1)
            return sync

        self.diag('    Syncing latest stored files...', 1)

        self._update_index_and_completeness(stored=True, local=False, cloud=False)

        stored_files = (
            self.data_index.loc[:, ['dropbox_file', 'stored_file', 'stored_file_complete', 'production_datetime', 'stored_file_exists']]
            .groupby('stored_file')
            .agg(dropbox_file=('dropbox_file', 'first'),
                 stored_file=('stored_file', 'first'),
                 stored_file_complete=('stored_file_complete', 'all'),
                 production_datetime=('production_datetime', 'min'),
                 stored_file_exists=('stored_file_exists', 'all'),
                 )
            .sort_values('production_datetime')
        )

        latest_stored_files = stored_files.loc[stored_files.stored_file_exists].tail(3)
        if latest_stored_files.empty:
            self.diag('        Nothing to upload.', 2)
            return sync

        dropbox_root_path = common_dropbox_root(latest_stored_files['dropbox_file'].tolist(), self._dropbox_root_path)

        client = self._get_dropbox_connection()
        remote_files = list_dropbox_files(client, dropbox_root_path)

        desired_remote_paths = {}
        to_upload = []
        for stored_file in latest_stored_files.index:
            remote_path = latest_stored_files.loc[stored_file, 'dropbox_file']
            desired_remote_paths[remote_path.lower()] = remote_path
            remote_metadata = remote_files.get(remote_path.lower())
            if not Path(stored_file).exists():
                continue
            if not compare_local_to_remote_hash(stored_file, remote_metadata):
                to_upload.append((stored_file, remote_path))

        stale_remote_paths = [
            metadata['path_display']
            for path_lower, metadata in remote_files.items()
            if path_lower not in desired_remote_paths
        ]

        if to_upload:
            self.diag(f'        Uploading ({self._dropbox_parallel_transfers} threads)...', 2)
            with UploadMonitor() as monitor:
                with ThreadPoolExecutor(max_workers=self._dropbox_parallel_transfers) as executor:
                    futures = {
                        executor.submit(upload_file, client, stored_file, remote_path): (stored_file, remote_path)
                        for stored_file, remote_path in to_upload
                    }
                    for future in as_completed(futures):
                        stored_file, remote_path = futures[future]
                        try:
                            future.result()
                        except Exception as ex:
                            print(f'        Error uploading {stored_file} -> {remote_path}: {ex}.')
                            continue

                        sync = True
                        self.diag('        ' + monitor.mark_success(stored_file, remote_path), 1)
        else:
            self.diag('        Dropbox files already match local storage.', 2)

        if stale_remote_paths:
            self.diag(f'        Deleting {len(stale_remote_paths)} stale Dropbox files...', 2)
            deleted_paths = delete_dropbox_paths(client, stale_remote_paths)
            if deleted_paths:
                sync = True
                for deleted_path in deleted_paths:
                    self.diag(f'        Deleted {deleted_path}', 1)

        if sync:
            self._update_index_and_completeness(stored=True, local=False, cloud=False)

        return sync

    def diag(self, msg:str, verbose:int) -> None:
        '''
        
        '''
        if self.verbose >= verbose:
            print(msg)

    def store(self) -> bool:
        '''
        Docstring for store
        Reads the data and saves it for storage. Shall be overloaded by product-specific tasks
        Should return a list of file paths (e.g., .parquet, .mr)
        '''
        
        stored = False

        self.diag('Storing...', 1)

        self.diag('    Building index and checking completeness...', 2)
        # Extend population to full storage files
        extended_index = self.populate(self.data_index['production_datetime'].min() - self._storage_search_window,
                              self.data_index['production_datetime'].max() + self._storage_search_window)
        self.data_index = extended_index.loc[extended_index['stored_file'].isin(self.data_index['stored_file'].unique())]
        self._clean_index()

        # Complete index
        self._update_index_and_completeness()

        self.diag('    Storing...', 2)
        stored_files = self.data_index.loc[~self.data_index['stored_file_complete'], 'stored_file'].unique()
        for s0 in stored_files[::-1]:
            # Collect data
            data = None
            already_stored = None
            if Path(s0).exists():
                self.diag(f'            Reading "{s0}" ({self.__class__.__name__})', 1)
                data = self._load_stored_file(s0)
                if data is not None:
                    already_stored = data.get_complete_index().stack()

            index = self.data_index[(self.data_index['stored_file']==s0)]
            index_existing = index.loc[self.data_index['data_exists'] & self.data_index['local_file_exists'], :]

            if already_stored is not None:
                idx = index_existing.set_index(['production_datetime', 'leadtime'], append=False, drop=False).idx.reindex(already_stored.loc[~already_stored].index).dropna()
                index_to_include = index_existing.loc[idx, :]
            else:
                index_to_include = index_existing

            local_files = index_to_include['local_file'].unique()
            for l1 in local_files:
                # Read file
                mr = self.read_local(l1)
                mr.verbose = False

                # Reduce footpring for storage (here to save memory)
                if not self.storage_bounding_box is None:
                    mr = mr.getCropped(
                        **{a:self.storage_bounding_box[k] for a, k in zip(['from_lat', 'to_lat', 'from_lon', 'to_lon'],
                                                                          ['south', 'north', 'west', 'east'])})
                
                # Join to previous reads
                if data is None:
                    data = mr
                else:
                    data.join(mr, strickt=True)

            if data is None:
                continue

            # Ensure completeness (production and leadtime)
            # np.full defaults to float64, which would promote a float32 raster (and
            # every later rewrite of the file, since join keeps the wider dtype).
            pad_dtype = np.result_type(data.data.dtype, np.float32)
                # Production dates
            production_datetimes = pd.DatetimeIndex(index['production_datetime'].unique())
            valid_production_datetimes = production_datetimes.isin(data.production_datetime)
            if not valid_production_datetimes.all():
                tmp = np.full([len(production_datetimes) if i==0 else data.data.shape[i] for i in range(5)], np.nan, dtype=pad_dtype)
                tmp[valid_production_datetimes, ...] = data.data
                data.data = tmp 
                data.production_datetime = production_datetimes

                # Leadtimes
            leadtimes = index['leadtime'].unique()
            if isinstance(leadtimes[0], pd.DateOffset):
                valid_leadtimes = pd.Index(leadtimes).isin(data.leadtimes)
            else:
                leadtimes = pd.to_timedelta(leadtimes)
                valid_leadtimes = leadtimes.isin(data.leadtimes)
            if not valid_leadtimes.all():
                tmp = np.full([len(valid_leadtimes) if i==2 else data.data.shape[i] for i in range(5)], np.nan, dtype=pad_dtype)
                tmp[:, :, valid_leadtimes, ...] = data.data
                data.data = tmp 
                data.leadtimes = leadtimes

            # Save file
            if not already_stored is None:
                newly_stored = data.get_complete_index().stack()
                if (already_stored==newly_stored).all():
                    # No changes to be saved
                    continue

            self.diag(f'            Saving "{s0}" ({self.__class__.__name__})', 1)
            data.save(s0)
            self.diag(f'                Done.', 1)
            stored = True

        # Update completeness
        self._update_index_and_completeness(local=False, cloud=False)

        return stored
        
    def _cleanup_old_files(self):
        self.diag('    Deleting old files...', 1)

        ctr = 0
        if self._max_local_age_months:
            max_local_age = pd.DateOffset(months=int(self._max_local_age_months))
            cutoff_date = pd.Timestamp.now() - max_local_age

            extended_index = self.populate(cutoff_date - pd.DateOffset(years=1))
            self.data_index = extended_index.loc[extended_index['stored_file'].isin(self.data_index['stored_file'].unique())]
            self._clean_index()
            self._update_index_and_completeness()

            to_delete = self.data_index.loc[(self.data_index.production_datetime<=cutoff_date) & self.data_index.local_file_exists, 'local_file'].unique()


            for f0 in to_delete:
                try:
                    Path(f0).unlink()
                    ctr += 1
                except Exception:
                    pass
        if ctr>0:
            self.diag(f'        Deleted {ctr} outdated local files.', 1)
        else:
            self.diag(f'        No local files to delete.', 1)

    def update(self, fail_if_older:bool=False) -> None:
        self._sync_latest_stored_download()

        self.retrieve(fail_if_older=fail_if_older)
        self.store()

        self._sync_latest_stored_upload()
        self._upload_to_cloud()

        self._cleanup_old_files()

    def _download_from_source(self) -> bool:
        '''
        To be overloaded for all classes
        '''

        return False

    def read_local(self, local_file: str) -> MeteoRaster:
        '''
        Returns a MeteoRaster object with the data.
        To be overloaded for all classes
        '''

        pass

    def check_local(self, local_file: str) -> bool:
        '''
        Checks if the local file is complete and valid.
        Can be overloaded.
        By default, read_local is used.
        '''

        return False

    def complete_local_files(self):
        '''
        Upkeeps files (following download from source)
        To be overloaded when required (e.g., in case of cumulative era5 variables)
        '''

        pass


