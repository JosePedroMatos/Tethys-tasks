from tethys_tasks import BaseTask, CaptureNewVariables, DownloadMonitor, create_kml_classes
import pandas as pd
import numpy as np
import xarray as xr
from pathlib import Path
import shutil
import tempfile
import os
import urllib.request
import urllib.error
import threading
from meteoraster import MeteoRaster
import numpy as np
from zipfile import ZipFile
from concurrent.futures import ThreadPoolExecutor, as_completed, ProcessPoolExecutor
from uuid import uuid4
from typing import Tuple

class GFS_025(BaseTask):
    '''
    Docstring for GFS 0.25 air temperature data for the caucasus region
    '''

    with CaptureNewVariables() as _GFS_025_VARIABLES: #It is essential that the format of the variable here is _CLASSnAME_VARIABLES
        PUBLICATION_LATENCY = pd.Timedelta(hours=8)
        PUBLICATION_MEMORY = pd.Timedelta(days=10)
        PRODUCTION_FREQUENCY = pd.Timedelta(hours=24)
        LEADTIMES = pd.timedelta_range('0h', '384h', freq='3h')

        SOURCE_PARALLEL_TRANSFERS = 3
        LOCAL_READ_PROCESSES = 2

        CLOUD_TEMPLATE = 'test/NOAA_GFS_0.25/%Y/%m/%H/{{leadtime_hours}}/gfs_4_%Y.%m.%d_%H_{{leadtime_hours}}.nc'
        LOCAL_PATH_TEMPLATE = 'NOAA_GFS_0.25/%Y/%m/%H/{{leadtime_hours}}/gfs_4_%Y.%m.%d_%H_{{leadtime_hours}}.nc'
        STORAGE_PATH_TEMPLATE = 'NOAA_GFS_0.25/gfs_{self._variable_lower}_{self._zone}/%Y/tethys_NOAA_GFS_0.25_{{floor_7_days}}.nct'

        STORAGE_SEARCH_WINDOW = pd.DateOffset(days=10)
        ASSUME_LOCAL_COMPLETE = True

        PIXEL_SIZE = 0.25

        GRIB_VARIABLE = dict(TMP='t2m',
                             PRATE='prate',
                             WEASD='sdwe',
                             SNOD='sde',
                             )
                
        BACKEND_KWARGS = dict(TMP={'filter_by_keys': {'stepType':'instant', 'typeOfLevel': 'heightAboveGround', 'level': 2}},
                              PRATE={'filter_by_keys': {'stepType':'instant', 'typeOfLevel': 'surface'}},
                              WEASD={'filter_by_keys': {'stepType':'instant', 'typeOfLevel': 'surface'}},
                              SNOD={'filter_by_keys': {'stepType':'instant', 'typeOfLevel': 'surface'}},
                              )

        UNITS = dict(TMP='C',
                     PRATE='mm/3h',
                     WEASD='unknown',
                     SNOD='unknown',
                     )

        ZONE = ''
        STORAGE_KML=''
        VARIABLE = ''
        VARIABLE_LOWER = ''

    def _7_days(self, production_datetime):
        reference = pd.Timestamp('1900-01-01')
        step = pd.Timedelta(days=7)
        return (reference + ((production_datetime - reference) // step) * step).dt.strftime('%Y.%m.%d')

    def _leadtime_hours(self, leadtime):
        return (leadtime // pd.Timedelta(hours=1)).astype(str).str.zfill(3)

    def populate(self, *args, **kwargs):
        # Add each 7 days (floor_7_days)
        # Add leadtime in 03 format hours (leadtime_hours)
        additional_columns = {'floor_7_days': lambda x: self._7_days(x['production_datetime']),
                              'leadtime_hours': lambda x: self._leadtime_hours(x['leadtime']),
                              }

        return super().populate(additional_columns=additional_columns, *args, **kwargs)

    def __download_helper(self, url: str, destination: str, production_datetime: pd.Timestamp, missing_state: dict, missing_lock: threading.Lock) -> Tuple[bool, str]:
        '''
        Download url into a system temp file and move to destination on success.
        '''

        dest_path = Path(destination)
        dest_path.parent.mkdir(parents=True, exist_ok=True)

        fd = None
        temp_file = None

        try:
            with missing_lock:
                missing_min = missing_state.get('min_missing')
            if missing_min is not None and production_datetime >= missing_min:
                return 'Skipping', str(dest_path)

            request = urllib.request.Request(
                url,
                headers={
                    'User-Agent': 'Mozilla/5.0',
                    'Accept': '*/*',
                    'Accept-Encoding': 'identity',
                },
            )
            # response = urllib.request.urlopen(request)
            with urllib.request.urlopen(request) as response:
                status = response.getcode()
                if status is not None and not (200 <= status < 300):
                    raise Exception(f'HTTP status {status}')

                fd, temp_path = tempfile.mkstemp(prefix=f'{dest_path.stem}.', suffix=f'.{uuid4().hex}.part')
                temp_file = Path(temp_path)

                with os.fdopen(fd, 'wb') as handle:
                    shutil.copyfileobj(response, handle)

            shutil.move(str(temp_file), str(dest_path))

            return 'Downloaded', str(dest_path)
        except urllib.error.HTTPError as ex:
            if ex.code == 403:
                # Old or future data
                with missing_lock:
                    current = missing_state.get('min_missing')
                    if current is None or production_datetime < current:
                        missing_state['min_missing'] = production_datetime
            return 'Not found', str(dest_path)
        except Exception as ex:
            if fd is not None:
                try:
                    os.close(fd)
                except OSError:
                    pass
            if temp_file is not None and temp_file.exists():
                temp_file.unlink()
            print(f'        Error downloading {url} -> {dest_path}: {ex}.')
            return 'Failed', str(dest_path)

    def _download_from_source(self) -> bool:
        '''
        Downloads missing files directly from the source

        Returns True of downloads were made
        '''

        self.diag('    Download from source...', 1)

        # ['production_datetime', 'leadtime', 'local_file']
        to_download = self.data_index.loc[~self.data_index['data_exists'], :]
        if to_download.shape[0]==0:
            self.diag('        Nothing to download.', 1)
            return False

        # Prepare urls
        url_template = 'https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl?file=gfs.t%Hz.pgrb2.0p25.f{{leadtime_hours}}&lev_10_m_above_ground=on&lev_2_m_above_ground=on&lev_surface=on&var_CPOFP=on&var_DPT=on&var_PRATE=on&var_RH=on&var_SNOD=on&var_SOILW=on&var_SUNSD=on&var_TCDC=on&var_TMP=on&var_UGRD=on&var_VGRD=on&var_WEASD=on&leftlon=0&rightlon=360&toplat=90&bottomlat=-90&dir=%%2Fgfs.%Y%m%d%%2F%H%%2Fatmos'

        url_template = url_template.format(self=self)
        to_download.loc[:, 'url'] = to_download['production_datetime'].dt.strftime(url_template)
        if '{' in url_template:
            to_download.loc[:, 'url'] = to_download.apply(lambda x: x['url'].format(**x.to_dict()), axis=1)

        self.diag(f'        Downloading ({self._source_parallel_transfers} threads).', 1)
        downloaded = False
        
        missing_state = {'min_missing': None}
        missing_lock = threading.Lock()
        
        with DownloadMonitor() as monitor:
            with ThreadPoolExecutor(max_workers=self._source_parallel_transfers) as executor:
                futures = {
                    executor.submit(
                        self.__download_helper,
                        r['url'],
                        r['local_file'],
                        r['production_datetime'],
                        missing_state,
                        missing_lock,
                    ): r['local_file']
                    for _, r in to_download.iterrows()
                }
                for future in as_completed(futures):
                    status, downloaded_file = future.result()
                    if status=='Downloaded':
                        msg = monitor.mark_success(downloaded_file)
                        self.diag('        ' + msg, 1)
                        downloaded = True
                    elif status=='Failed':
                        self.diag(f'        Download failed for {futures[future]}.', 1)
                    elif status=='Not found':
                        self.diag(f'        Not found. Skipping beyond {futures[future]}.', 1)

        if downloaded:
            self._check_existing_data(stored=False, cloud=False)

        return downloaded

    def read_local(self, local_file: str) -> MeteoRaster:

        self.diag(f'            Reading "{local_file}" ({self.__class__.__name__})', 1)

        data, units = self._read_local(
            local_file,
            self._variable,
            self._backend_kwargs,
            self._grib_variable,
            self._leadtimes,
            self._units,
        )
        mr = MeteoRaster(data=data, units=units, variable=self._variable, verbose=False)
        return mr
    
    @staticmethod
    def _read_local(
        local_file: str,
        variable: str,
        backend_kwargs: dict,
        grib_variable: dict,
        leadtimes: pd.TimedeltaIndex | None = None,
        units: dict | None = None,
        return_slice_only: bool = False,
    ):
        '''
        Returns a MeteoRaster-compatible data dict (and units) or a data slice.
        '''

        with open(local_file, "rb") as f:
            tmp = f.read(4)
        if tmp == b'\x89HDF' or tmp == b'CDF':
            file_type = 'netcdf'
        elif tmp == b'GRIB':
            file_type = 'grib'
        else:
            raise Exception(f'Unknown file type in GFS "{local_file}": "{tmp}"')

        if file_type=='netcdf':
            with xr.open_dataset(local_file, engine='netcdf4') as ds:
                # raise Exception('NetCDF support to be checked')
                latitudes = ds.lat.data
                longitudes = ds.lon.data
                production_datetime = np.array(pd.to_datetime(ds.productionDatetimes.data))
                leadtimes_local = pd.arrays.TimedeltaArray._from_sequence([pd.Timedelta(hours=ds.leadtimes.data[0]/1E9/3600)])
                data_ = ds[variable][...].data
        elif file_type=='grib':
            with xr.open_dataset(local_file, engine='cfgrib', **backend_kwargs[variable]) as ds:
                latitudes = ds.latitude.data
                longitudes = ds.longitude.data
                production_datetime = pd.to_datetime(np.expand_dims(ds.time.data, 0))
                leadtimes_local = pd.to_timedelta(pd.to_datetime(np.expand_dims(ds.valid_time.data, 0))-production_datetime[0])

                data_ = np.array(ds[grib_variable[variable]][...].values)

                if variable=='TMP':
                    data_ -= 273.15

        if variable=='PRATE':
            data_ *= 3*3600

        if return_slice_only:
            return data_

        if leadtimes is None or units is None:
            raise Exception('leadtimes and units are required unless return_slice_only=True')

        data = np.full([1, 1, leadtimes.shape[0]] + list(data_.shape), np.NaN)
        lt_idx = np.searchsorted(leadtimes, leadtimes_local[0])
        data[0, 0, lt_idx, ...] = data_
        leadtimes = leadtimes

        data = dict(data=data,
                  latitudes=latitudes,
                  longitudes=longitudes,
                  production_datetime=production_datetime,
                  leadtimes=leadtimes,
                  )

        units = units[variable]

        return data, units

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
            index_existing = index.loc[self.data_index['data_exists'] & self.data_index['local_file_exists'], :].copy()

            if not index_existing.empty:
                self.diag(f'            Reading {len(index_existing)} local files for "{s0}"', 1)

                # Initialize with first file
                # Use _read_local to get dimensions and units
                first_row = index_existing.iloc[0]
                d_sample, units = self._read_local(
                    first_row['local_file'],
                    self._variable,
                    self._backend_kwargs,
                    self._grib_variable,
                    self._leadtimes,
                    self._units,
                )
                lats = d_sample['latitudes']
                lons = d_sample['longitudes']
                
                # Prepare Aggregation Array
                unique_prods = np.sort(index['production_datetime'].unique())
                unique_leads = self._leadtimes
                
                # [Prod, Ens, Lead, Lat, Lon]
                full_data = np.full((len(unique_prods), 1, len(unique_leads), len(lats), len(lons)), np.nan, dtype=np.float32)

                # Mappings
                prod_map = {pd.Timestamp(p): i for i, p in enumerate(unique_prods)}
                lead_map = {pd.Timedelta(l): i for i, l in enumerate(unique_leads)}
                
                index_existing.set_index(['production_datetime', 'leadtime'], drop=False, inplace=True)
                index_existing['already_stored'] = False
                if not already_stored is None:
                    index_existing.loc[index_existing.index.intersection(already_stored[already_stored].index), 'already_stored'] = True
                index_existing.set_index(['idx'], drop=False, inplace=True)
                
                index_existing = index_existing.loc[~index_existing['already_stored'], :]

                if index_existing.shape[0]>0:
                    read_rows = list(index_existing.itertuples(index=False))

                    with ProcessPoolExecutor(max_workers=self._local_read_processes) as executor:
                        futures = {}
                        for row in read_rows:
                            future = executor.submit(
                                self._read_local,
                                row.local_file,
                                self._variable,
                                self._backend_kwargs,
                                self._grib_variable,
                                None,
                                None,
                                True,
                            )
                            futures[future] = row

                        for future in as_completed(futures):
                            row = futures[future]
                            self.diag(f'            Read "{row.local_file}" ({self.__class__.__name__})', 1)
                            data_slice = future.result()

                            # Place into aggregated array
                            p_idx = prod_map[row.production_datetime]
                            l_idx = lead_map[row.leadtime]
                            full_data[p_idx, 0, l_idx, :, :] = data_slice

                    # Create MeteoRaster from aggregated data
                    data_dict = dict(data=full_data,
                                    latitudes=lats,
                                    longitudes=lons,
                                    production_datetime=unique_prods,
                                    leadtimes=unique_leads,
                                    )
                    
                    mr = MeteoRaster(data=data_dict, units=units, variable=self._variable, verbose=False)

                    # Crop (only once)
                    if not self.storage_bounding_box is None:
                        mr = mr.get_cropped(
                            **{a:self.storage_bounding_box[k] for a, k in zip(['from_lat', 'to_lat', 'from_lon', 'to_lon'],
                                                                            ['south', 'north', 'west', 'east'])})
                    
                    # Join with previously stored data
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

# creates regional classes such as GFS_025_TMP_CAUCASUS, GFS_025_PMP_CAUCASUS, etc...
variable_kwargs = {'VARIABLE': ['TMP', 'PCP'], 'VARIABLE_LOWER': ['tmp', 'pcp']}
create_kml_classes(GFS_025, variable_kwargs)

if __name__=='__main__':
    import matplotlib.pyplot as plt
    plt.ion()

    task = GFS_025_TMP_CAUCASUS(download_from_source=False, date_from='2026-02-10')
    # task = GFS_025_PCP_CAUCASUS(download_from_source=False, date_from='2025-01-01')

    task.retrieve_and_upload()
    task.store()

    # task.retrieve_store_and_upload()

    # task = GFS_025_PCP_BELGIUM(download_from_source=False, date_from='2026-01-01')    
    # task.retrieve_and_upload()
    # task.retrieve()
    # task.upload_to_cloud()

    pass