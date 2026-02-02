
import pandas as pd
import os
from pathlib import Path
from tethys_tasks import CaptureNewVariables, running_in_docker, DownloadMonitor, UploadMonitor
from collections.abc import Iterable
import xml.etree.ElementTree as ET
import numpy as np
from meteoraster import MeteoRaster

from azure.storage.blob import BlobServiceClient
from azure.core.credentials import AzureSasCredential
from concurrent.futures import ThreadPoolExecutor, as_completed

class CompletenessIndex():

    def __init__(self, folder:Path):
        self.folder = folder
        self.index_file = folder / 'completeness.csv'

        self.folder.mkdir(exist_ok=True, parents=True)
        
        self.index = pd.Series([], index=pd.Index([], name='file_name'), name='complete')

        self.read()
        self.check_existance()
    
    def read(self):
        if self.index_file.exists():
            self.index = pd.read_csv(self.index_file, sep=',', index_col='file_name')

            if isinstance(self.index, pd.DataFrame):
                if self.index.shape[1]==1:
                    self.index = self.index.iloc[:, 0]
                else:
                    self.index = pd.Series([], index=pd.Index([], name='file_name'), name='complete')
                    return

            if not isinstance(self.index, pd.Series) or self.index.name != 'complete' or self.index.index.name != 'file_name':
                self.index = pd.Series([], index=pd.Index([], name='file_name'), name='complete')

    def check_existance(self):
        for f0 in self.index.index:
            if not (self.folder / f0).exists():
                self.index[f0] = False

    def write(self):
        self.index = self.index[self.index==True]
        try:
            if self.index.empty:
                if self.index_file.exists():
                    self.index_file.unlink()
            else:
                self.index.to_csv(self.index_file, sep=',')
        except Exception as ex:
            print(f'Update of completeness index failed: {self.index_file.parent.absolute()} ({ex}).')
            

    def remove(self, files:Iterable):
        for f0 in files:
            if f0 in self.index.index:
                self.index[f0] = False

    def include(self, files:Iterable):
        self.index = self.index.reindex(files, fill_value=True)

    def get_complete(self):
        return self.index[self.index].index.tolist()

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

        LOCAL_STORAGE_FOLDER = __LOCAL_STORAGE_FOLDER
        TRANSFER_FOLDER = __TRANSFER_FOLDER

        PUBLICATION_LATENCY = pd.Timedelta(days=0)
        PRODUCTION_FREQUENCY = pd.Timedelta(days=1)
        LEADTIMES = pd.timedelta_range('0d', '0d', freq='1h')
        CLEANUP_WINDOW = pd.DateOffset(months=2)
        CLOUD_PARALLEL_TRANSFERS = int(os.getenv('CLOUD_PARALLEL_TRANSFERS', 6))
        SOURCE_PARALLEL_TRANSFERS = 1

        STORAGE_SEARCH_WINDOW = pd.DateOffset(months=14)

        VARIABLE=''
        SOURCE_KML = ''
        STORAGE_KML = ''
        ZONE = ''
        PIXEL_SIZE = None

        CLOUD_TEMPLATE = f'test/ERA5_{VARIABLE.upper()}/era5_{VARIABLE}_{ZONE}/%Y/era5_{VARIABLE}_%Y.%m.zip'
        LOCAL_PATH_TEMPLATE = f'ERA5_{VARIABLE.upper()}/era5_{VARIABLE}_{ZONE}/%Y/era5_{VARIABLE}_%Y.%m.zip'
        STORAGE_PATH_TEMPLATE = f'ERA5_{VARIABLE.upper()}/era5_{VARIABLE}_{ZONE}/%Y/tethys_era5_{VARIABLE}_%Y.%m.01.mr'

        DATE_FROM = '2021-04-15'

    def __init__(self, download_from_source=False, date_from:str='', date_to:str='', verbose=2, *args, **kwargs):
        '''
        Docstring for __init__
        
        kwargs that match class variables (such as CLEANUP_WINDOW) are localized (and eventually superseeded by kwargs) as _*lowercase* versions,
        For example, CLEANUP_WINDOW, cleanup_window, or Cleanup_Window will all be accessible as self._cleanup_window.

        All other kwargs are defined as self. properties without changes.
        '''

        self.verbose = verbose
        self.diag(f'Initializing...', 1)
        self.diag(f'    Running from docker: {self.__DOCKER}', 1)

        self._set_base_variables(BaseTask, kwargs)
        self.blob_service_client = None
        self.container_client = None

        self._variable_upper = self._variable.upper()
        self._cloud_template = self._cloud_template.format(self=self)
        self._local_path_template = self._local_path_template.format(self=self)
        self._storage_path_template = self._storage_path_template.format(self=self)

        self.download_from_source = download_from_source

        self.source_bounding_box = None
        if self._source_kml.endswith('.kml'):
            self.source_bounding_box = self._get_bounding_box(self._source_kml, self._pixel_size)
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

        index = index.assign(
            event_datetime=index['production_datetime'] + index['leadtime'],
            doy=index.production_datetime.dt.day_of_year,
        )

        if isinstance(index['leadtime'][0], pd.Timedelta):
            index = index.assign(
                lt_days=(index['leadtime'] / pd.Timedelta(days=1)).astype('int64'),
                lt_hours=(index['leadtime'] / pd.Timedelta(hours=1)).astype('int64'),
            )
        elif isinstance(index['leadtime'][0], pd.DateOffset):
            #created as this [pd.DateOffset(months=i) for i in range(7)] (always defined in months for simplicity)
            index = index.assign(
                lt_months=index['leadtime'].apply(lambda x: x.months),
                lt_years=index['leadtime'].apply(lambda x: x.months) / 12,
            )

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

    def retrieve(self, *args, **kwargs):
        '''
        Docstring for retrieve
        
        :param file_path: Description
        '''

        self.diag('Retrieving...', 1)

        self._update_index_and_completeness()

        downloaded = self._download_from_cloud()

        if self.download_from_source:
            downloaded = self._download_from_source()
            if downloaded:
                self._update_index_and_completeness(stored=False, cloud=False)

        self.complete_local_files()

        self._update_index_and_completeness(stored=False, cloud=False)

        self.diag('    Done retrieving.', 1)

    def _check_existing_files(self, stored:bool=True, local:bool=True, cloud:bool=True) -> None:
        '''
        Updates the data_index dataframe.

        Checks what files exist in storage, locally, or in the cloud, in that order.
        Once found in a "higher" order, the data is not looked for in lower orders.
            For example, data already in storage will not be subject to search locally or in the cloud.

        False may appear even if files do exist, if they have not been checked (stored -> local -> cloud).

        The updated dataframe will have the following fields updated:
            cloud_file_exists
            local_file_exists
            stored_file_exists
            data_exists (partial update, only in the case of complete files)
        '''

        # Stored files
        if stored:
            stored_files = self.data_index['stored_file'].unique()
            self.data_index.loc[:, 'stored_file_exists'] = False
            for f0 in stored_files:
                self.data_index.loc[self.data_index['stored_file']==f0, 'stored_file_exists'] = Path(f0).exists()

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
            for f0 in local_files:
                self.data_index.loc[self.data_index['local_file']==f0, 'local_file_exists'] = Path(f0).exists()
                
            for folder in set([Path(f).parent for f in local_files]):
                ci = CompletenessIndex(folder)
                for name in ci.get_complete():
                    for l0 in local_files:
                        local_file = None
                        if l0.endswith(name):
                            local_file = l0
                            break
                    if local_file:
                        self.data_index.loc[self.data_index['local_file']==local_file, 'local_file_complete'] = True

        # Cloud files
        if cloud:
            cloud_files = self.data_index.loc[~self.data_index['stored_file_exists'] & ~self.data_index['local_file_exists'], 'cloud_file'].unique()
            self.data_index.loc[:, 'cloud_file_exists'] = False
            cloud_files_exist = self._check_cloud(cloud_files)
            for f0, exists in zip(cloud_files, cloud_files_exist):
                self.data_index.loc[self.data_index['cloud_file']==f0, 'cloud_file_exists'] = exists

    def _check_existing_data(self, stored:bool=True, local:bool=True, cloud:bool=True, thorough:bool=False, check_files:bool=True, **kwargs) -> None:
        '''
        Checks data one file at a time
        Updates an index file that marks complete files when there are changes (per folder)
        '''

        if check_files:
            self._check_existing_files(stored=stored, local=local, cloud=cloud, **kwargs)

        self.data_index.set_index(['production_datetime', 'leadtime'], append=False, drop=False, inplace=True)

        previous_files = self.__get_files_by_production_datetime(self.data_index['production_datetime'].min()-self._production_frequency)
        posterior_files = self.__get_files_by_production_datetime(self.data_index['production_datetime'].max()+self._production_frequency)

        # Storage files
        if stored:
            stored_files = self.data_index.loc[self.data_index['stored_file_exists'], 'stored_file'].unique()
            for s0 in stored_files[::-1]:
                if not thorough:
                    idx = self.data_index['stored_file']==s0
                    if self.data_index.loc[idx, 'stored_file_complete'].all():
                        self.data_index.loc[idx, 'data_exists'] = True
                        continue

                if MeteoRaster.get_completeness(s0):
                    self.data_index.loc[idx, 'data_exists'] = True
                    self.data_index.loc[idx, 'stored_file_complete'] = True
                else:
                    self.data_index.loc[idx, 'stored_file_complete'] = False
                    complete_index = MeteoRaster.load(s0, verbose=False).get_complete_index().stack()
                    
                    idx = complete_index.index.isin(self.data_index.index)
                    complete_index = complete_index.loc[idx, :]

                    self.data_index.loc[complete_index.index, 'data_exists'] = complete_index | self.data_index.loc[complete_index.index, 'data_exists']

        # Local files
        if local:
            local_files = self.data_index.loc[self.data_index['local_file_exists'], 'local_file'].unique()
            for l0 in local_files[::-1]:
                idx = self.data_index['local_file']==l0
                if not thorough:
                    if self.data_index.loc[idx, 'local_file_complete'].all():
                        self.data_index.loc[idx, 'data_exists'] = True
                        continue
                
                data = self.read_local(l0)
                axes = (1, 3, 4)
                data_steps = pd.DataFrame(np.sum(np.isfinite(data.data), axis=axes)>0,
                                          index=pd.DatetimeIndex(data.production_datetime, name='production_datetime'),
                                          columns=pd.Index(data.leadtimes, name='leadtime')).stack()
                valid_steps = data_steps[data_steps]
                valid_steps = valid_steps.loc[valid_steps.index.isin(self.data_index.index)]
                self.data_index.loc[valid_steps.index, 'data_exists'] = valid_steps | self.data_index.loc[valid_steps.index, 'data_exists']

                complete_index = self.data_index.loc[idx, 'local_file_complete']
                complete_index.loc[valid_steps.index] = True
                self.data_index.loc[idx, 'local_file_complete'] = complete_index.all()

            # Fix completeness at the edges (maybe False when complete, but the check has not been made)
            self.data_index.loc[self.data_index['local_file']==previous_files['local_file'], 'local_file_complete'] = False
            self.data_index.loc[self.data_index['local_file']==posterior_files['local_file'], 'local_file_complete'] = False

        self.data_index.set_index(['idx'], append=False, drop=False, inplace=True)

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
            stored_files = self.data_index.groupby('stored_file').last()[['stored_file_complete']]
            stored_files.loc[:, 'folder'] = [Path(f).parent for f in stored_files.index]
            stored_files = stored_files.reset_index().set_index(['folder', 'stored_file'])
            for folder in stored_files.index.get_level_values('folder').unique():
                ci = CompletenessIndex(folder)
                to_remove = []
                to_include = []
                for stored_file, complete in stored_files.loc[folder, :].iterrows():
                    if complete.iloc[0]:
                        to_include.append(Path(stored_file).name)
                    else:
                        to_remove.append(Path(stored_file).name)
                ci.include(to_include)
                ci.remove(to_remove)
                ci.write()

        if local:
            local_files = self.data_index.groupby('local_file').last()[['local_file_complete']]
            local_files.loc[:, 'folder'] = [Path(f).parent for f in local_files.index]
            local_files = local_files.reset_index().set_index(['folder', 'local_file'])
            for folder in local_files.index.get_level_values('folder').unique():
                ci = CompletenessIndex(folder)
                to_remove = []
                to_include = []
                for local_file, complete in local_files.loc[folder, :].iterrows():
                    if complete.iloc[0]:
                        to_include.append(Path(local_file).name)
                    else:
                        to_remove.append(Path(local_file).name)
                ci.include(to_include)
                ci.remove(to_remove)
                ci.write()

    def _update_index_and_completeness(self, stored:bool=True, local:bool=True, cloud:bool=True, thorough=False) -> None:
        '''
        Checks files and data
        Update the completeness files
        '''

        self._check_existing_data(stored=stored, local=local, cloud=cloud, thorough=thorough)
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
        
        tree = ET.parse(kml_file)
        root = tree.getroot()
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

    def upload_to_cloud(self, thorough:bool=False) -> bool:
        '''
        Docstring for upload_to_cloud
        
        :param self: Description
        :param args: Description
        :param kwargs: Description
        '''

        self.diag('    Uploading to Azure...', 1)

        self.diag('        Building index...', 2)
        self._clean_index()

        self._update_index_and_completeness(stored=False, cloud=False, thorough=thorough)

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
        uploaded = False
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

    def diag(self, msg:str, verbose:int) -> None:
        '''
        
        '''
        if self.verbose >= verbose:
            print(msg)

    def retrieve_and_upload(self) -> None:
        self.retrieve()
        self.upload_to_cloud()

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
                data = MeteoRaster.load(s0, verbose=False)
                already_stored = data.get_complete_index().stack()

            index = self.data_index[(self.data_index['stored_file']==s0)]
            index_existing = index.loc[self.data_index['data_exists'] & self.data_index['local_file_exists'], :]

            local_files = index_existing['local_file'].unique()
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
                    data.join(mr)

            if data is None:
                continue

            # Ensure completeness (production and leadtime)
                # Production dates
            production_datetimes = index['production_datetime'].unique()
            valid_production_datetimes = production_datetimes.isin(data.production_datetime)
            if not valid_production_datetimes.all():
                tmp = np.full([len(production_datetimes) if i==0 else data.data.shape[i] for i in range(5)], np.nan)
                tmp[valid_production_datetimes, ...] = data.data
                data.data = tmp 
                data.production_datetime = production_datetimes

                # Leadtimes
            leadtimes = index['leadtime'].unique()
            if isinstance(leadtimes[0], pd.DateOffset):
                pass
            else:
                leadtimes = pd.to_timedelta(leadtimes)
            valid_leadtimes = leadtimes.isin(data.leadtimes)
            if not valid_leadtimes.all():
                tmp = np.full([len(valid_leadtimes) if i==1 else data.data.shape[i] for i in range(5)], np.nan)
                tmp[:, valid_leadtimes, ...] = data.data
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

    def complete_local_files(self):
        '''
        Upkeeps files (following download from source)
        To be overloaded when required (e.g., in case of cumulative era5 variables)
        '''

        pass