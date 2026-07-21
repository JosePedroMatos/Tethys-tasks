from tethys_tasks import BaseTask, CaptureNewVariables, DownloadMonitor, create_kml_classes, running_in_docker
import pandas as pd
import xarray as xr
from pathlib import Path
import shutil
import tempfile
import os
import urllib.request
import urllib.error
import ssl
import threading
import http.cookiejar
# import time
from meteoraster import MeteoRaster
import numpy as np
from concurrent.futures import ThreadPoolExecutor, as_completed
from uuid import uuid4
from typing import Tuple
import base64

class GPM_IMERG_FINAL(BaseTask):
    '''
    GPM IMERG Final Run (30 min) — downloaded from server.
    '''

    with CaptureNewVariables() as _GPM_IMERG_FINAL_VARIABLES: #It is essential that the format of the variable here is _CLASSnAME_VARIABLES
        PUBLICATION_LATENCY = pd.DateOffset(months=4)
        PRODUCTION_FREQUENCY = pd.Timedelta(minutes=30)
        LEADTIMES = pd.timedelta_range('0h', '0h', freq='1h') 

        SOURCE_PARALLEL_TRANSFERS = 1
        LOCAL_READ_PROCESSES = 2
        
        # CLOUD_UPLOAD_LOCAL = True

        SOURCE = os.getenv('IMERG_SOURCE', 'OPeNDAP') # ['OPeNDAP', 'PPS']

        FILE_TEMPLATE = '3B-HHR.MS.MRG.3IMERG.%Y%m%d-S%H%M%S-E{{end_str}}.{{dly_mins}}.{{version}}.HDF5.nc4'

        DOWNLOAD_TEMPLATE_OPeNDAP = 'https://gpm1.gesdisc.eosdis.nasa.gov/opendap/GPM_L3/GPM_3IMERGHH.07/%Y/%j/' + FILE_TEMPLATE[:-3] + 'dap.nc4?dap4.ce=/lat[0:1:1799];/lon[0:1:3599];/time[0:1:0];/lat_bnds[0:1:1799][0:1:1];/precipitation[0:1:0][0:1:3599][0:1:1799];/lon_bnds[0:1:3599][0:1:1]'
        DOWNLOAD_TEMPLATE_PPS = 'https://arthurhouhttps.pps.eosdis.nasa.gov/gpmdata/%Y/%m/%d/imerg/' + FILE_TEMPLATE.replace('.HDF5.nc4', '.HDF5')

        DOWNLOAD_TEMPLATE = DOWNLOAD_TEMPLATE_OPeNDAP if SOURCE=='OPeNDAP' else DOWNLOAD_TEMPLATE_PPS
        
        CLOUD_TEMPLATE = 'IMERG_FINAL/%Y/%j/' + FILE_TEMPLATE
        LOCAL_PATH_TEMPLATE = 'IMERG_FINAL/%Y/%m/%d/' + FILE_TEMPLATE
        STORAGE_PATH_TEMPLATE = 'IMERG_FINAL/imerg_final_{self._zone}/{{floor_year}}/tethys_imerg_final_{{floor_7_days}}.nct'

        STORAGE_SEARCH_WINDOW = pd.DateOffset(days=9)
        ASSUME_LOCAL_COMPLETE = True

        FAIL_IF_OLDER = pd.DateOffset(months=6)
        
        EARTH_DATA_USER = os.getenv('EARTH_DATA_USER')
        EARTH_DATA_PASSWORD = os.getenv('EARTH_DATA_PASSWORD')
        PPS_USER = os.getenv('PPS_USER')

        DISABLE_CERT_VERIFY = os.getenv('PPS_DISABLE_CERT_VERIFY', 'false').lower() in ('1', 'true', 'yes')

        PIXEL_SIZE = 0.1

        ZONE = 'world'
        VARIABLE = 'precipitation'
        UNITS = 'mm/30min'

        VERSION_DICT = {
            '1900-01-01': 'V07B',
            '2025-10-01': 'V07C',
            }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
                    
        if self._source=='OPeNDAP':
            self._download_template = self._download_template_opendap
            auth_str = f'{self._earth_data_user}:{self._earth_data_password}'

        elif self._source=='PPS':
            self._download_template = self._download_template_pps
            auth_str = f'{self._pps_user}:{self._pps_user}'

        else:
            raise Exception(f'    Source {self._source} unknown... ({self.__class__.__name__})')

        auth_bytes = auth_str.encode('ascii')
        auth_b64 = base64.b64encode(auth_bytes).decode('ascii')
        self.basic_auth = f'Basic {auth_b64}'

        if self._disable_cert_verify:
            print('        WARNING: PPS_DISABLE_CERT_VERIFY is set — SSL certificate verification is DISABLED for GPM downloads.')

    @staticmethod
    def _7_days(production_datetime):
        reference = pd.Timestamp('1900-01-01')
        step = pd.Timedelta(days=7)
        return (reference + ((production_datetime - reference) // step) * step).dt.strftime('%Y.%m.%d')

    @staticmethod
    def _floor_year(production_datetime):
        reference = pd.Timestamp('1900-01-01')
        step = pd.Timedelta(days=7)
        return (reference + ((production_datetime - reference) // step) * step).dt.strftime('%Y')
    
    @staticmethod
    def _end_str(production_datetimes) -> str: 
        end_str = (production_datetimes + pd.Timedelta(seconds=1799)).dt.strftime('%H%M%S')
        return end_str

    @staticmethod
    def _dly_mins(production_datetimes) -> str: 
        minutes = production_datetimes.dt.hour * 60 + production_datetimes.dt.minute
        dly_mins = minutes.astype(str).str.zfill(4)
        return dly_mins

    @staticmethod
    def _version(production_datetimes, version_dict) -> str:

        version = pd.Series('', index=production_datetimes.index)
        for date_str, ver in sorted(version_dict.items()):
            version[production_datetimes >= pd.Timestamp(date_str)] = ver
        return version

    def populate(self, *args, **kwargs):
        # Add end time str (end_str)
        # Add daily hours str (dly_hrs)
        additional_columns = {'end_str': lambda x: self._end_str(x['production_datetime']),
                              'dly_mins': lambda x: self._dly_mins(x['production_datetime']),
                              'floor_7_days': lambda x: self._7_days(x['production_datetime']),
                              'floor_year': lambda x: self._floor_year(x['production_datetime']),
                              'version': lambda x: self._version(x['production_datetime'], self._version_dict),
                              }

        return super().populate(additional_columns=additional_columns, *args, **kwargs)

    def __download_helper(self, url: str, destination: str, production_datetime: pd.Timestamp, missing_state: dict, missing_lock: threading.Lock, fatal_event: threading.Event) -> Tuple[str, str]:
        """Download url into a system temp file and move to destination on success."""

        dest_path = Path(destination)
        dest_path.parent.mkdir(parents=True, exist_ok=True)

        fd = None
        temp_file = None
        try:
            if fatal_event.is_set():
                # A fatal (e.g. server certificate) error already occurred elsewhere:
                # skip without hitting the network again.
                return 'Skipping', str(dest_path)

            with missing_lock:
                missing_min = missing_state.get('min_missing')
            if missing_min is not None and production_datetime >= missing_min:
                return 'Skipping', str(dest_path)

            # Create a CookieJar to handle the redirects (NASA Earthdata requires cookies)
            cj = http.cookiejar.CookieJar()
            handlers = [urllib.request.HTTPCookieProcessor(cj)]
            if self._disable_cert_verify:
                unverified_context = ssl.create_default_context()
                unverified_context.check_hostname = False
                unverified_context.verify_mode = ssl.CERT_NONE
                handlers.append(urllib.request.HTTPSHandler(context=unverified_context))
            opener = urllib.request.build_opener(*handlers)

            request = urllib.request.Request(
                url,
                headers={
                    'User-Agent': 'Mozilla/5.0',
                    'Authorization': self.basic_auth
                },
            )
            with opener.open(request) as response:
                status = response.getcode()
                if status is not None and not (200 <= status < 300):
                    raise Exception(f'HTTP status {status}')

                fd, temp_path = tempfile.mkstemp(prefix=f'{dest_path.stem}.', suffix=f'.{uuid4().hex}.part')
                temp_file = Path(temp_path)

                with os.fdopen(fd, 'wb') as handle:
                    shutil.copyfileobj(response, handle)

            shutil.move(temp_file, dest_path)
            return 'Downloaded', str(dest_path)
        except urllib.error.HTTPError as ex:
            if ex.code == 404:
                with missing_lock:
                    current = missing_state.get('min_missing')
                    if current is None or production_datetime < current:
                        missing_state['min_missing'] = production_datetime
                return 'Not found', str(dest_path)
            else:
                return f'Failed ({ex.code})', str(dest_path)
        except Exception as ex:
            if fd is not None:
                try:
                    os.close(fd)
                except OSError:
                    pass
            if temp_file is not None and temp_file.exists():
                temp_file.unlink()

            if isinstance(ex, urllib.error.URLError) and isinstance(ex.reason, ssl.SSLCertVerificationError):
                # Server-side certificate problem, not a per-file issue: stop other queued
                # downloads from hitting the network too, and let it propagate so the DAG
                # task fails instead of silently marking files as failed.
                fatal_event.set()
                raise

            print(f'        Error downloading {url} -> {dest_path}: {ex}.')
            return 'Failed', str(dest_path)

    def _download_from_source(self) -> bool:
        '''
        Downloads missing files directly from the source
        '''
        self.diag('    Download from source...', 1)

        download_dates = self.data_index.loc[~self.data_index['data_exists'], 'production_datetime'].unique()

        if len(download_dates) == 0:
            self.diag('        Nothing to download.', 1)
            return False

        to_download = self.data_index.loc[self.data_index['production_datetime'].isin(download_dates), :].copy()

        download_template = self._download_template.format(self=self)
        to_download.loc[:, 'url'] = to_download['production_datetime'].dt.strftime(download_template)
        if '{' in download_template:
            to_download['url'] = to_download.apply(lambda x: x['url'].format(**x.to_dict()), axis=1)
        
        self.diag(f'        Downloading ({self._source_parallel_transfers} threads).', 1)
        downloaded = False

        missing_state = {'min_missing': None}
        missing_lock = threading.Lock()
        fatal_event = threading.Event()

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
                        fatal_event,
                    ): r['local_file']
                    for _, r in to_download.iterrows()
                }
                for future in as_completed(futures):
                    status, downloaded_file = future.result()
                    if status=='Downloaded':
                        msg = monitor.mark_success(downloaded_file)
                        self.diag('        ' + msg, 1)
                        downloaded = True
                    elif status.startswith('Failed'):
                        self.diag(f'        Download failed for {futures[future]}.', 1)
                    elif status.startswith('Not found'):
                        self.diag(f'        Not found. Skipping beyond {futures[future]}.', 1)

        if downloaded:
            self._check_existing_data(stored=False, cloud=False)

        return downloaded

    @staticmethod
    def _read_local_helper(local_file: str) -> dict:
        '''
        Read a local IMERG file into raw arrays (no MeteoRaster).
        '''

        try:
            with xr.open_dataset(local_file, group='Grid', engine='h5netcdf') as ds:
                longitudes = np.round(ds['lon'].values, 3)
                latitudes = np.round(ds['lat'].values, 3)
                data_array = np.expand_dims(ds['precipitation'].values[0,...].transpose(), (0, 1, 2))
                
                time_val = ds['time'].values[0]
                production_datetime = pd.to_datetime([time_val.strftime('%Y-%m-%d %H:%M:%S')])
        except Exception as ex:
            with xr.open_dataset(local_file, engine='h5netcdf', decode_times=False) as ds:
                longitudes = np.round(ds['lon'].values, 3)
                latitudes = np.round(ds['lat'].values, 3)
                data_array = np.expand_dims(ds['precipitation'].values[0, ...].transpose(), (0, 1, 2))

                # time is stored as seconds since Unix epoch (1970-01-01 00:00:00 UTC)
                time_seconds = float(ds['time'].values[0])
                production_datetime = pd.to_datetime(
                    [pd.Timestamp('1970-01-01', tz='UTC') + pd.Timedelta(seconds=time_seconds)]
                ).tz_localize(None)
        
        data = {
            'latitudes': latitudes,
            'longitudes': longitudes,
            'production_datetime': production_datetime,
            'leadtimes': pd.to_timedelta([0], unit='h'),
            'data': data_array
        }

        return data

    # HDF5 signature (also the leading bytes of NetCDF4/.nc4 files, which are HDF5 under the hood)
    _HDF5_SIGNATURE = b'\x89HDF\r\n\x1a\n'

    @classmethod
    def _is_corrupt_hdf5(cls, local_file: str) -> bool:
        '''
        Confirms whether a local file is actually corrupt (as opposed to a transient read
        error) so it can be safely deleted. Returns True only for confirmed-bad files.

        A file is considered corrupt when it is empty, does not start with the HDF5
        signature (e.g. an HTML/error page saved as data), or cannot be opened even though
        the signature is present (internal truncation). Returns False if the file is missing
        (nothing to delete).
        '''

        path = Path(local_file)
        if not path.exists():
            return False

        try:
            if path.stat().st_size == 0:
                return True

            with open(path, 'rb') as f:
                header = f.read(len(cls._HDF5_SIGNATURE))
            if header != cls._HDF5_SIGNATURE:
                return True
        except OSError:
            # Could not even stat/read the first bytes -> treat as corrupt.
            return True

        # Signature is present: confirm it is truly unreadable (e.g. truncated internals).
        try:
            with xr.open_dataset(local_file, engine='h5netcdf'):
                pass
            return False
        except Exception:
            return True

    def _read_local_safe(self, local_file: str) -> dict:
        '''
        Resilient wrapper around _read_local_helper: never lets a bad file block the run.
        Returns the data dict on success, or None on failure (deleting the file only when
        corruption is confirmed, so it is re-downloaded naturally on the next update).
        '''

        try:
            return self._read_local_helper(local_file)
        except Exception as ex:
            self.diag(f'            Failed to read "{local_file}" ({self.__class__.__name__}): {ex}.', 1)

            if self._is_corrupt_hdf5(local_file):
                try:
                    Path(local_file).unlink(missing_ok=True)
                    self.diag(f'            Deleted corrupt file "{local_file}". It will re-download on the next update.', 1)
                except OSError as del_ex:
                    self.diag(f'            Could not delete corrupt file "{local_file}": {del_ex}.', 1)
            else:
                self.diag(f'            "{local_file}" failed to read but is not confirmed corrupt; skipping without deleting.', 1)

            return None

    def read_local(self, local_file: str) -> MeteoRaster:
        '''
        Returns a MeteoRaster object with the data
        '''
        self.diag(f'            Reading "{local_file}" ({self.__class__.__name__})', 1)

        data = self._read_local_safe(local_file)
        if data is None:
            raise Exception(f'Could not read local file "{local_file}" ({self.__class__.__name__}).')

        mr = MeteoRaster(data, units=self._units, variable=self._variable, verbose=False)
        return mr

    def store(self) -> bool:
        '''
        Reads the data and saves it for storage, aggregating by storage file.
        Concatenation is done over production_datetime (axis 0 of data['data']).
        '''

        stored = False

        self.diag('Storing...', 1)

        self.diag('    Building index and checking completeness...', 2)
        extended_index = self.populate(
            self.data_index['production_datetime'].min() - self._storage_search_window,
            self.data_index['production_datetime'].max() + self._storage_search_window,
        )
        self.data_index = extended_index.loc[
            extended_index['stored_file'].isin(self.data_index['stored_file'].unique())
        ]
        self._clean_index()
        self._update_index_and_completeness()

        self.diag('    Storing...', 2)
        stored_files = self.data_index.loc[
            ~self.data_index['stored_file_complete'], 'stored_file'
        ].unique()

        for s0 in stored_files[::-1]:
            data = None
            already_stored = None
            if Path(s0).exists():
                self.diag(f'            Reading "{s0}" ({self.__class__.__name__})', 1)
                data = MeteoRaster.load(s0, verbose=False)
                already_stored = data.get_complete_index().stack()

            index = self.data_index[(self.data_index['stored_file'] == s0)]
            index_existing = index.loc[
                self.data_index['data_exists'] & self.data_index['local_file_exists'],
                :
            ].copy()

            if not index_existing.empty:
                self.diag(f'            Reading {len(index_existing)} local files for "{s0}"', 1)

                # Find the first readable file for the lat/lon sample; corrupt ones are
                # deleted inside _read_local_safe and skipped.
                d_sample = None
                for sample_file in index_existing['local_file']:
                    d_sample = self._read_local_safe(sample_file)
                    if d_sample is not None:
                        break

                if d_sample is None:
                    self.diag(f'            No readable local files for "{s0}"; skipping.', 1)
                    continue

                lats = d_sample['latitudes']
                lons = d_sample['longitudes']

                unique_prods = np.sort(index['production_datetime'].unique())
                unique_leads = self._leadtimes

                full_data = np.full(
                    (len(unique_prods), 1, len(unique_leads), len(lats), len(lons)),
                    np.nan,
                    dtype=np.float32,
                )

                prod_map = {pd.Timestamp(p): i for i, p in enumerate(unique_prods)}
                lead_map = {pd.Timedelta(l): i for i, l in enumerate(unique_leads)}

                index_existing.set_index(['production_datetime', 'leadtime'], drop=False, inplace=True)
                index_existing['already_stored'] = False
                if already_stored is not None:
                    index_existing.loc[
                        index_existing.index.intersection(already_stored[already_stored].index),
                        'already_stored'
                    ] = True
                index_existing.set_index(['idx'], drop=False, inplace=True)
                index_existing = index_existing.loc[~index_existing['already_stored'], :]

                if index_existing.shape[0] > 0:
                    read_rows = list(index_existing.itertuples(index=False))

                    # if running_in_docker():
                    #     self.diag('        Running in Docker, using single process for local reads.', 1)
                    #     for row in read_rows:
                    #         self.diag(f'            Read "{row.local_file}" ({self.__class__.__name__})', 1)
                    #         data_slice = self._read_local_helper(row.local_file)

                    #         p_idx = prod_map[row.production_datetime]
                    #         full_data[p_idx, 0, 0, :, :] = data_slice['data']
                            
                    #         self.diag('            Sleeping 2 seconds...', 1)
                    #         time.sleep(2)

                            
                    # else:
                    with ThreadPoolExecutor(max_workers=self._local_read_processes) as executor:
                        futures = {executor.submit(self._read_local_safe, row.local_file): row for row in read_rows}

                        for future in as_completed(futures):
                            row = futures[future]
                            self.diag(f'            Read "{row.local_file}" ({self.__class__.__name__})', 1)
                            data_slice = future.result()

                            if data_slice is None:
                                self.diag(f'            Skipping unreadable file "{row.local_file}".', 1)
                                continue

                            p_idx = prod_map[row.production_datetime]
                            full_data[p_idx, 0, 0, :, :] = data_slice['data']

                    data_dict = dict(
                        data=full_data,
                        latitudes=lats,
                        longitudes=lons,
                        production_datetime=unique_prods,
                        leadtimes=unique_leads,
                    )
                    mr = MeteoRaster(data=data_dict, units=self._units, variable=self._variable, verbose=False)

                    if self.storage_bounding_box is not None:
                        mr = mr.get_cropped(
                            **{
                                a: self.storage_bounding_box[k]
                                for a, k in zip(
                                    ['from_lat', 'to_lat', 'from_lon', 'to_lon'],
                                    ['south', 'north', 'west', 'east'],
                                )
                            }
                        )

                    if data is None:
                        data = mr
                    else:
                        data.join(mr)

            if data is None:
                continue

            production_datetimes = index['production_datetime'].unique()
            valid_production_datetimes = production_datetimes.isin(data.production_datetime)
            if not valid_production_datetimes.all():
                tmp = np.full(
                    [len(production_datetimes) if i == 0 else data.data.shape[i] for i in range(5)],
                    np.nan,
                )
                tmp[valid_production_datetimes, ...] = data.data
                data.data = tmp
                data.production_datetime = production_datetimes

            # leadtimes = index['leadtime'].unique()
            # valid_leadtimes = leadtimes.isin(data.leadtimes)
            # if not valid_leadtimes.all():
            #     tmp = np.full(
            #         [len(valid_leadtimes) if i == 1 else data.data.shape[i] for i in range(5)],
            #         np.nan,
            #     )
            #     tmp[:, valid_leadtimes, ...] = data.data
            #     data.data = tmp
            #     data.leadtimes = leadtimes

            if already_stored is not None:
                newly_stored = data.get_complete_index().stack()
                try:
                    if (already_stored == newly_stored).all():
                        continue
                except Exception:
                    pass

            self.diag(f'            Saving "{s0}" ({self.__class__.__name__})', 1)
            data.save(s0)
            self.diag('                Done.', 1)
            stored = True

        self._update_index_and_completeness(local=False, cloud=False)

        return stored

class GPM_IMERG_LATE(GPM_IMERG_FINAL):
    '''
    Docstring for GPM IMERG Final Run (30 min) precipitation data
    '''

    with CaptureNewVariables() as _GPM_IMERG_LATE_VARIABLES: #It is essential that the format of the variable here is _CLASSnAME_VARIABLES
        PUBLICATION_LATENCY = pd.DateOffset(hours=12)
        
        FILE_TEMPLATE = '3B-HHR-L.MS.MRG.3IMERG.%Y%m%d-S%H%M%S-E{{end_str}}.{{dly_mins}}.{{version}}.HDF5.nc4'

        DOWNLOAD_TEMPLATE_OPeNDAP = 'https://gpm1.gesdisc.eosdis.nasa.gov/opendap/hyrax/GPM_L3/GPM_3IMERGHHL.07/%Y/%j/' + FILE_TEMPLATE[:-3] + 'dap.nc4?dap4.ce=/lat[0:1:1799];/lon[0:1:3599];/time[0:1:0];/lat_bnds[0:1:1799][0:1:1];/precipitation[0:1:0][0:1:3599][0:1:1799];/lon_bnds[0:1:3599][0:1:1]'
        DOWNLOAD_TEMPLATE_PPS = 'https://jsimpsonhttps.pps.eosdis.nasa.gov/imerg/late/%Y%m/' + FILE_TEMPLATE.replace('.HDF5.nc4','.RT-H5')

        CLOUD_TEMPLATE = 'IMERG_LATE/%Y/%j/' + FILE_TEMPLATE
        LOCAL_PATH_TEMPLATE = 'IMERG_LATE/%Y/%m/%d/' + FILE_TEMPLATE
        STORAGE_PATH_TEMPLATE = 'IMERG_LATE/imerg_late_{self._zone}/{{floor_year}}/tethys_imerg_late_{{floor_7_days}}.nct'

        FAIL_IF_OLDER = pd.DateOffset(hours=18)

        VERSION_DICT = {
            '1900-01-01': 'V07B',
            # '2026-03-03': 'V07C',
            }

# creates regional classes such as GPM_IMERG_FINAL_CAUCASUS, GPM_IMERG_LATE_CAUCASUS, etc...
create_kml_classes(GPM_IMERG_FINAL)
create_kml_classes(GPM_IMERG_LATE)

if __name__=='__main__':
    import matplotlib.pyplot as plt
    plt.ion()

    # task = GPM_IMERG_FINAL_ZAMBEZI(download_from_origin=True, date_from='2025-09-25 00:00')
    # task.retrieve_store_upload_and_cleanup()

    # date_from = '2020-01-01 00:00:00'

    # date_from = '2025-09-01 00:00:00'
    # date_from = '2026-01-25 00:00:00'
    date_from = '2025-07-01 00:00:00'

    task = GPM_IMERG_FINAL_IBERIA(download_from_origin=True, date_from=date_from)


    # task = GPM_IMERG_LATE_BELGIUM(download_from_origin=True, date_from=date_from)
    # task = GPM_IMERG_LATE_CAUCASUS(download_from_origin=True, date_from=date_from)
    # task = GPM_IMERG_LATE_SWITZERLAND(download_from_origin=True, date_from=date_from)
    # task = GPM_IMERG_LATE_TAJIKISTAN(download_from_origin=True, date_from=date_from)
    # task = GPM_IMERG_LATE_ZAMBEZI(download_from_origin=True, date_from=date_from)
    task.update()


    # docker-compose run --rm tethys-tasks GPM_IMERG_LATE_TAJIKISTAN update --class_kwargs "{\"download_from_origin\": \"False\", \"date_from\": \"'2026-06-01'\"}"

    pass