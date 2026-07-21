from tethys_tasks import BaseTask, CaptureNewVariables, create_kml_classes
import pandas as pd
import xarray as xr
from pathlib import Path
from collections.abc import Iterable
import cdsapi
import shutil
import calendar
import tempfile
from meteoraster import MeteoRaster
import numpy as np
from zipfile import ZipFile
from concurrent.futures import ThreadPoolExecutor, as_completed
import random
import string
import os
import atexit

# ---------------------------------------------------------------------------
# Session-persistent unpack cache (ERA5 only).
# Each local zip is extracted at most once per process into a uniquely named
# "era5_*" folder in the system temp dir; reads reuse the unpacked files instead
# of re-extracting the multi-GB grib on every call. The folder is erased at
# process exit (and, additionally, at the end of each ERA5.update()).
# ---------------------------------------------------------------------------
_ERA5_UNPACK_ROOT = None


def _era5_unpack_root() -> Path:
    '''Lazily create (once per process) and return the session unpack root.'''
    global _ERA5_UNPACK_ROOT
    if _ERA5_UNPACK_ROOT is None or not _ERA5_UNPACK_ROOT.exists():
        _ERA5_UNPACK_ROOT = Path(tempfile.mkdtemp(prefix='era5_'))
    return _ERA5_UNPACK_ROOT


def _clear_era5_unpack_root() -> None:
    '''Erase the session unpack root (re-created lazily on the next read).'''
    global _ERA5_UNPACK_ROOT
    if _ERA5_UNPACK_ROOT is not None and _ERA5_UNPACK_ROOT.exists():
        shutil.rmtree(_ERA5_UNPACK_ROOT, ignore_errors=True)
    _ERA5_UNPACK_ROOT = None


atexit.register(_clear_era5_unpack_root)


class ERA5(BaseTask):
    '''
    Docstring for ERA5
    '''

    with CaptureNewVariables() as _ERA5_VARIABLES: #It is essential that the format of the variable here is _CLASSnAME_VARIABLES
        PUBLICATION_LATENCY = pd.Timedelta(days=6)
        PRODUCTION_FREQUENCY = pd.Timedelta(hours=1)
        LEADTIMES = pd.timedelta_range('0D', '0D', freq='1h')

        SOURCE_PARALLEL_TRANSFERS = 3

        # CDS download log verbosity: 'silent' | 'info' | 'debug' (env-overridable).
        CDS_VERBOSITY = os.getenv('CDS_VERBOSITY', 'info').lower()
        # CDS download progress bar (ignored when CDS_VERBOSITY == 'silent').
        CDS_PROGRESS = os.getenv('CDS_PROGRESS', 'False').lower() in ('true', '1', 't')

        PIXEL_SIZE = 0.1

        ASSUME_LOCAL_COMPLETE = False

        ERA5_LOCAL_WORLD = os.getenv('ERA5_LOCAL_WORLD', 'False').lower() in ('true', '1', 't')

        if ERA5_LOCAL_WORLD:
            CLOUD_TEMPLATE = 'ERA5_{self._variable_upper}/era5_{self._variable}_world/%Y/era5_{self._variable}_%Y.%m.zip'
            LOCAL_PATH_TEMPLATE = 'ERA5_{self._variable_upper}/era5_{self._variable}_world/%Y/era5_{self._variable}_%Y.%m.zip'
        else:
            CLOUD_TEMPLATE = 'ERA5_{self._variable_upper}/era5_{self._variable}_{self._zone}/%Y/era5_{self._variable}_%Y.%m.zip'
            LOCAL_PATH_TEMPLATE = 'ERA5_{self._variable_upper}/era5_{self._variable}_{self._zone}/%Y/era5_{self._variable}_%Y.%m.zip'
        STORAGE_PATH_TEMPLATE = 'ERA5_{self._variable_upper}/era5_{self._variable}_{self._zone}/%Y/tethys_era5_{self._variable}_%Y.%m.01.nct'

        STORAGE_SEARCH_WINDOW = pd.DateOffset(days=40)


        VARIABLE_DICT = dict(
            t2m = '2m_temperature',
            tp = 'total_precipitation',
            sd = 'snow_depth_water_equivalent',
            u10 = '10m_u_component_of_wind',
            v10 = '10m_v_component_of_wind',
            ssr = 'surface_net_solar_radiation',
        )

        CUMULATIVE = dict(
            tp=True,
            ssr=True,
            u10=False,
            v10=False,
            sd=False,
            t2m=False,
        )

        FAIL_IF_OLDER = pd.Timedelta('8D')

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # store previous local file (for completeness of cumulative variables)
        index = self.populate(self.data_index['production_datetime'].min() - pd.DateOffset(years=1), self.data_index['production_datetime'].min(), silent=True)
        self.previous_local_file = index.loc[index['local_file']!=self.data_index['local_file'].iloc[0], 'local_file'].iloc[-1]

    def _cds_client(self):
        '''
        Builds a cdsapi client honouring CDS_VERBOSITY ('silent'|'info'|'debug')
        and CDS_PROGRESS. 'silent' disables both logging and the progress bar.
        Note: cdsapi's progress bar defaults to on and is NOT coupled to quiet, so
        it must be turned off explicitly (a plain quiet=True still shows the bar).
        '''
        verbosity = str(self._cds_verbosity).lower()
        progress = self._cds_progress
        if isinstance(progress, str):
            progress = progress.lower() in ('true', '1', 't')
        if verbosity == 'silent':
            return cdsapi.Client(quiet=True, progress=False)
        if verbosity == 'debug':
            return cdsapi.Client(debug=True, progress=progress)
        return cdsapi.Client(progress=progress)

    def __download_CDS(self, variables):
        '''
        Downloads data from CDS
        To be used in parallel by a ThreadPool
        '''

        options, local_path = variables
        local_path_ = Path(local_path)

        c = self._cds_client()
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_file_path = Path(temp_file.name)
        try:
            c.retrieve('reanalysis-era5-land', options).download(temp_file_path)
            if local_path_.exists():
                local_path_.unlink()
            try:
                with tempfile.TemporaryDirectory() as temp_dir:
                    with ZipFile(temp_file_path, 'r') as zip_ref:
                        zip_ref.extractall(temp_dir)
                    grib_files = list(Path(temp_dir).glob('*.grib'))
                    ERA5._read_file(grib_files[0])
                temp_file_path.replace(local_path_)
            except OSError as ex:
                shutil.copyfile(temp_file_path, local_path_)
                temp_file_path.unlink(missing_ok=True)
            return ((True, local_path))
        except Exception as ex:
            print(ex)
            return ((False, local_path))

    def _download_from_source(self) -> bool:
        '''
        Downloads missing files directly from the source

        Returns True of downloads were made
        '''

        self.diag('    Download from source...', 1)

        data_to_retrieve_from_source = self.data_index.loc[~self.data_index['data_exists'], :] 
        months_to_download = data_to_retrieve_from_source['production_datetime'].dt.strftime('%Y-%m')

        if len(months_to_download)==0:
            self.diag('        Nothing to download.', 1)
            return False

        info = []
        for _, i0 in months_to_download.reset_index().groupby(by='production_datetime').first().iterrows():
            template_row = self.data_index.loc[i0.iloc[0]]
            
            local_path = template_row['local_file']
            Path(local_path).parent.mkdir(parents=True, exist_ok=True)

            date = template_row['production_datetime'].replace(day=1, hour=0)
            days = ['%02d' % d for d in range(1, calendar.monthrange(date.year, date.month)[1]+1)]
            options = {'data_format': 'grib',
                       'year': f'{date.year}',
                       'month': f'{date.month:02d}',
                       'day': days,
                       'variable': [self._variable_dict[self._variable]],
                       'download_format': 'zip',
                       'time': [f'{h:02d}:00' for h in range(24)],
                       'nocache': ''.join(random.choice(string.digits) for _ in range(6))
                       }
            if self._era5_local_world:
                area = {}
            else:
                area = dict(area=[self.source_bounding_box[d] for d in ['north', 'west', 'south', 'east']])
            options.update(area)
            variables = ((options, local_path))
            info.append(variables)

        self.diag(f'        Downloading ({self._source_parallel_transfers} threads).', 1)
        downloaded = False
        results = []
        with ThreadPoolExecutor(max_workers=self._source_parallel_transfers) as executor:
            futures = [executor.submit(self.__download_CDS, i) for i in info[::-1]]
            for future in as_completed(futures):
                state, local_path_ = future.result()
                if state:
                    self.data_index.loc[self.data_index['local_file']==local_path_, 'local_file_exists'] = True
                    downloaded = True
                else:
                    print(f'Download failed: {Path(local_path_).name}')

        return downloaded

    @staticmethod
    def _read_file(grib_file:str, variable:str='', bounded=False, bounding_box=None, just_start=False) -> dict:

        data = {}
        with xr.open_dataset(grib_file, engine='cfgrib', indexpath='', chunks={"time": 24}) as ds:

            if variable=='':
                variable_list = list(ds.data_vars)
                if len(variable_list)>1:
                    raise Exception('The file should not have more than one data variable.')
                variable = variable_list[0]

            ds_ = ds
            if just_start:
                # Read only the first time step (kept full-world when not bounded).
                ds_ = ds_.isel(time=slice(0, 1))
            if bounded and bounding_box:
                # Crop to the storage region BEFORE compute() so only the region is
                # materialised (a full-world month would otherwise exhaust memory).
                # Use the SAME rounded lat/lon test as MeteoRaster.getCropped so the grid
                # matches files stored via getCropped: a plain .sel(slice) uses exact float
                # comparison and drops boundary rows whose grid value is a hair outside the
                # bound (e.g. 30.0 stored as 29.9999999999), giving an off-by-one row/col.
                lat = np.round(ds_.latitude.values, 6)
                lat_mask = (lat >= round(bounding_box['south'], 6)) & (lat <= round(bounding_box['north'], 6))
                ds_ = ds_.isel(latitude=np.nonzero(lat_mask)[0])
                west = round(bounding_box['west'] % 360, 6)
                east = round(bounding_box['east'] % 360, 6)
                lon = np.round(ds_.longitude.values, 6)
                if west <= east:
                    lon_mask = (lon >= west) & (lon <= east)
                else:
                    # region straddles the 0deg/360deg meridian
                    lon_mask = (lon >= west) | (lon <= east)
                ds_ = ds_.isel(longitude=np.nonzero(lon_mask)[0])

            data['latitudes'] = ds_.latitude.data
            data['longitudes'] = ds_.longitude.data
            data['production_datetime'] = ds_.time.data
            if isinstance(data['production_datetime'], np.datetime64):
                data['production_datetime'] = np.array([data['production_datetime']])
            data['steps'] = ds_.step.data
            # data['data'] = ds_[variable].compute(scheduler='single-threaded').values
            data['data'] = ds_[variable].compute().values

        if variable not in ['sd'] and len(data['steps'])<24:
            raise Exception(f'The downloaded data does not have the expected number of time steps.')

        return data

    def _read_helper(self, grib_file:str, bounded=False) -> dict:
        '''
        Reads one grib file
        '''

        try:
            data = self._read_file(grib_file, self._variable, bounded=bounded, bounding_box=self.storage_bounding_box)
        except Exception as ex:
            raise Exception(f'{str(ex)} ({self.__class__.__name__}).')

        if self._cumulative[self._variable] and len(data['data'].shape)!=4:
            raise Exception(f'The downloaded data does not have the expected number of dimensions {self.__class__.__name__}.')
        
        return data

    def update(self, *args, **kwargs) -> None:
        '''
        Overloads update() only to erase the session unpack cache at the end of
        the run (atexit is the backstop for other entry points).
        '''
        try:
            super().update(*args, **kwargs)
        finally:
            _clear_era5_unpack_root()

    def _ensure_unpacked(self, local_file) -> Path:
        '''
        Extracts a local zip into the session unpack folder at most once (keyed on
        the grib CRC, so appending a small parquet/csv does not trigger a costly
        re-extraction; a re-downloaded zip changes the grib CRC and is re-extracted).
        Returns the folder that mirrors the zip contents.
        '''
        local_file = Path(local_file)
        folder = _era5_unpack_root() / local_file.stem
        marker = folder / '.grib_crc'
        with ZipFile(local_file, 'r') as z:
            grib_info = next((i for i in z.infolist() if i.filename.endswith('.grib')), None)
            if grib_info is None:
                raise ValueError(f'Expected a .grib in {local_file}.')
            crc = str(grib_info.CRC)
            if folder.exists() and marker.exists() and marker.read_text() == crc and list(folder.glob('*.grib')):
                return folder
            if folder.exists():
                shutil.rmtree(folder, ignore_errors=True)
            folder.mkdir(parents=True, exist_ok=True)
            z.extractall(folder)
        marker.write_text(crc)
        return folder

    @staticmethod
    def _step_index_name(local_file) -> str:
        return Path(local_file).stem + '_index.csv'

    def _read_step_index(self, local_file):
        '''
        Returns the cached set of valid (production_datetime, leadtime) steps for a
        local file as an all-True pd.Series, or None if no cache exists. Reads only
        the small csv (from the unpacked folder if present, else straight from the
        zip entry) -- never decompresses the grib.
        '''
        local_file = Path(local_file)
        csv_name = self._step_index_name(local_file)
        folder_csv = _era5_unpack_root() / local_file.stem / csv_name
        df = None
        if folder_csv.exists():
            df = pd.read_csv(folder_csv)
        else:
            try:
                with ZipFile(local_file, 'r') as z:
                    if csv_name in z.namelist():
                        with z.open(csv_name) as f:
                            df = pd.read_csv(f)
            except Exception:
                df = None
        if df is None or df.empty:
            return None
        prod = pd.to_datetime(df['production_datetime'])
        lead = pd.to_timedelta(df['leadtime'])
        idx = pd.MultiIndex.from_arrays([prod, lead], names=['production_datetime', 'leadtime'])
        return pd.Series(True, index=idx)

    def _write_step_index(self, local_file, valid_steps_full) -> None:
        '''
        Persists the file's full set of valid (production_datetime, leadtime) steps
        as a small csv, both in the unpacked folder and appended into the zip so it
        survives across sessions.
        '''
        local_file = Path(local_file)
        csv_name = self._step_index_name(local_file)
        try:
            folder = _era5_unpack_root() / local_file.stem
            folder.mkdir(parents=True, exist_ok=True)
            folder_csv = folder / csv_name
            valid_steps_full.index.to_frame(index=False).to_csv(folder_csv, index=False)
            with ZipFile(local_file, 'a') as z:
                if csv_name not in z.namelist():
                    z.write(folder_csv, arcname=csv_name)
        except Exception as ex:
            print(f'Creation of step-index csv failed ({csv_name}) ({self.__class__.__name__}): {ex}.')

    def read_local(self, local_file: str, bounded=True) -> MeteoRaster:
        '''
        Returns a MeteoRaster object with the ERA5 Land data
        '''
        
        self.diag(f'            Reading "{local_file}" ({self.__class__.__name__})', 1)

        if not Path(local_file).exists():
            raise Exception('Local file does not exit.')

        folder = self._ensure_unpacked(local_file)

        grib_files = list(folder.glob('*.grib'))
        parquet_files = list(folder.glob('*.parquet'))
        if len(grib_files) != 1:
            raise ValueError(f'Expected exactly one .grib file in {local_file}, found {len(grib_files)}.')
        else:
            grib_file = grib_files[0]
        parquet_file = None
        if len(parquet_files) ==1:
            parquet_file = parquet_files[0]
        elif len(parquet_files)>=1:
            raise ValueError(f'Expected exactly one .parquet file in {local_file}, found {len(parquet_files)}.')

        data = self._read_helper(grib_file, bounded=bounded)

        if parquet_file is None:
            parquet_file_ = Path(local_file).with_suffix('.parquet')
            if parquet_file_.exists():
                parquet_file = parquet_file_

        if parquet_file is not None and Path(parquet_file).exists():
            last_cum_step = pd.read_parquet(parquet_file)
            # Labels are stored as strings with latitude as a data column (parquet
            # forbids float labels and fastparquet drops a string index); restore the
            # float lat/lon grid so the reindex below aligns to the grib.
            last_cum_step = last_cum_step.set_index('latitude')
            last_cum_step.index = last_cum_step.index.astype(float)
            last_cum_step.columns = last_cum_step.columns.astype(float)
            if last_cum_step.shape != data['data'].shape[-2:]:
                # Parquet is stored full-world (shared across regions via the world
                # file); restrict it to the region actually read.
                last_cum_step = last_cum_step.reindex(
                    index=data['latitudes'], columns=data['longitudes'])
            if last_cum_step.shape != data['data'].shape[-2:]:
                raise Exception(f'ERA5 Land ({self._variable}) processing failed. Lat and Lon of downloaded and stored files do not match.')
            data['data'][-1, -1, ...] = last_cum_step.values

        # Capture the FULL-WORLD first step (just_start -> one step, memory-safe) so
        # cumulative variables can write the shared full-world boundary parquet even
        # when the main read above was cropped to a region.
        first_cum_step = None
        if self._cumulative[self._variable]:
            fs = self._read_file(grib_file, self._variable, bounded=False, just_start=True)
            first_cum_step = pd.DataFrame(
                fs['data'][0, -1, :, :],
                index=pd.Index(fs['latitudes'], name='latitude'),
                columns=pd.Index(fs['longitudes'], name='longitude'),
            )

        if self._cumulative[self._variable]:
            '''
            tp files do not include the last step of the requested period.
            They contain the last cum value of the previous day instead at row 0.
            This is a problem difficult to surmount within this routine (not all data is present).

            00:00 holds the sum of the previous day
            '''
            
            # Save parquet with the first step if the previous local file is not complete
            local_file_idx = self.data_index.loc[self.data_index['local_file']==local_file, 'idx'].iloc[0]
            if local_file_idx==0:
                previous_file = self.previous_local_file
            else:
                previous_file = self.data_index.loc[self.data_index['idx']==local_file_idx-1, 'local_file'].iloc[0]
            
            if Path(previous_file).exists():
                with ZipFile(previous_file, mode='a') as zip_file:
                    names = zip_file.namelist()
                    for name in names:
                        if name.endswith('.parquet'):
                            previous_file = None
                            break

            if previous_file:
                try:    
                    parquet_path = Path(previous_file)
                    parquet_path = parquet_path.with_suffix('.parquet')
                    parquet_path.parent.mkdir(parents=True, exist_ok=True)
                    if parquet_path.exists():
                        parquet_path.unlink()

                    # Parquet forbids float labels (fastparquet raises on float
                    # columns and drops a string index), so store lat/lon labels as
                    # strings and keep latitude as a data column; read_local restores
                    # the float lat/lon grid.
                    prev_cum_step = first_cum_step.copy()
                    prev_cum_step.index = prev_cum_step.index.astype(str)
                    prev_cum_step.columns = prev_cum_step.columns.astype(str)
                    prev_cum_step = prev_cum_step.reset_index()

                    prev_cum_step.to_parquet(parquet_path, index=False)
                except Exception as ex:
                    print(f'        Creation of .parquet file failed ({parquet_path}) ({self.__class__.__name__}): {ex}.')

            # (date, hour, ...) > (timestamp, ---)
            data['data'] = np.diff(data['data'], n=1, axis=1, prepend=0)
    
                
        if self._variable == 'tp':
            data['data'] *= 1000
            units = 'mm/hr'
        elif self._variable == 't2m':
            data['data'] -= 273.15
            units = 'C'
        elif self._variable == 'sd':
            data['data'] *= 1000
            units = 'mm'
        elif self._variable == 'ssr':
            data['data'] /= 3600
            units = 'W/m2'
        elif self._variable == 'u10' or self._variable == 'v10':
            units = 'm/s'

        if not isinstance(data['steps'], Iterable):
            data['steps'] = [data['steps']]
            
        if self._variable not in ['sd']:
            data['data'] = np.reshape(data['data'], (data['data'].shape[0]*data['data'].shape[1], data['data'].shape[-2], data['data'].shape[-1]))
        
            times = np.tile(data['production_datetime'], (24, 1)).transpose() + np.tile(data['steps'], (data['production_datetime'].shape[0], 1))
            data['production_datetime'] = times.ravel()
        
        if self._cumulative[self._variable]:
            data['production_datetime'] -= data['production_datetime'][1] - data['production_datetime'][0]
        
        data['data'] = np.expand_dims(data['data'], [1, 2])
        data['leadtimes'] = np.array([pd.Timedelta('0D')])
                
        tmp = MeteoRaster(data, units=units, variable=self._variable, verbose=False)
        tmp.trim()
        # tmp.getDataFromLatLon(-30, 30).to_clipboard(excel=True)
        
        return tmp

    def complete_local_files(self):
        '''
        Upkeeps files which are not complete but for which the data exists (only cumulative variables)
        Reads each file, extracts the valid first time step and saves it as an appendix to the previous .zip file with raw data
        Deletes all used .parquet files
        '''
        
        if self._cumulative[self._variable]:

            complete = self.data_index.groupby('local_file')[['data_exists', 'local_file_exists']].all()
            complete = complete.apply(lambda x: x.all(), axis=1)

            for f0 in complete.index[complete].tolist():
                # All the data is available. Check .parquet

                with ZipFile(f0, mode='a') as zip_file:
                    names = zip_file.namelist()
                    has_parquet = any(n.endswith('.parquet') for n in names)
                    local_parquet = Path(f0).with_suffix('.parquet')
                    if not has_parquet:
                        if not local_parquet.exists():
                            # Try to create the local .parquet by reading the next localfile
                            reference_datetime = self.data_index.loc[self.data_index['local_file']==f0, 'production_datetime'].max()
                            index = self.populate(reference_datetime, reference_datetime + pd.DateOffset(years=1))
                            next_local_file = index.loc[index['local_file']!=f0, 'local_file'].iloc[0]
                            if Path(next_local_file).exists():
                                self.read_local(next_local_file)

                        if local_parquet.exists():
                            try:
                                # keep the unpacked folder in sync (avoids a grib re-extract)
                                folder = _era5_unpack_root() / Path(f0).stem
                                if folder.exists():
                                    shutil.copy(local_parquet, folder / local_parquet.name)
                                zip_file.write(local_parquet, arcname=local_parquet.name)
                                local_parquet.unlink()
                            except Exception as ex:
                                print(f'        Error appending .parquet to {Path(f0).name}: {ex}.')
                        else:
                            self.data_index.loc[self.data_index['local_file']==f0, 'local_file_complete'] = False
                    else:
                        if local_parquet.exists():
                            try:
                                local_parquet.unlink()
                            except Exception as ex:
                                print(f'        Error removing .parquet for {Path(f0).name}: {ex}.')

    def _check_existing_data(self, *args, **kwargs):
        '''
        Overloads the base code and adds a check for local file completeness on cumulative variables (.grib and .parquet present)
        '''

        # Runs the base code
        super()._check_existing_data(*args, **kwargs)

        # Verifies that all cumulative files are complete
        if self._cumulative[self._variable]:
            self.diag('        Verifying completeness for ERA5 cumulative variables...', 2)

            local_files = self.data_index.loc[self.data_index['local_file_complete'], 'local_file']
            for f0 in local_files:
                    with ZipFile(f0, mode='r') as zip_file:
                        names = zip_file.namelist()
                    # A cumulative file is complete only if it carries the boundary
                    # parquet (the step-index csv is ignored by this check).
                    if not any(n.endswith('.parquet') for n in names):
                        self.data_index.loc[self.data_index['local_file']==f0, 'local_file_complete'] = False

            self._update_completeness(stored=False)

    def read_local_completeness(self, local_file:str) -> pd.DataFrame:
        '''
        Returns a pd.Series with the valid steps of a local file
        [production_datetime  leadtime] [Bool]

        Can be overloaded when a full read is not necessary
        '''

        valid_steps_full = self._read_step_index(local_file)
        if valid_steps_full is None:
            # Cache miss: decode the grib once, compute per-step validity, and cache it.
            data = self.read_local(local_file, bounded=True)
            axes = (1, 3, 4)
            data_steps = pd.DataFrame(np.sum(np.isfinite(data.data), axis=axes)>0,
                                    index=pd.DatetimeIndex(data.production_datetime, name='production_datetime'),
                                    columns=pd.Index(data.leadtimes, name='leadtime')).stack()
            valid_steps_full = data_steps[data_steps]
            self._write_step_index(local_file, valid_steps_full)

        valid_steps = valid_steps_full.loc[valid_steps_full.index.isin(self.data_index.index)]

        return valid_steps

# creates regional classes such as ERA5_CAUCASUS_TP, ERA5_CAUCASUS_T2M, TAJIKISTAN_T2M, etc...
create_kml_classes(ERA5, {'VARIABLE': ['tp', 't2m', 'sd']})

if __name__=='__main__':
    import matplotlib.pyplot as plt
    plt.ion()

    # path = r'T:\tethys-tasks local\ERA5_TP\era5_tp_caucasus'
    # removes_file_from_zip(path)

    # path = r'T:\tethys-tasks local\ERA5_SD'
    # rename_lowercase(path)

    kwargs = dict(download_from_origin=False,
        date_from='2000-01-01',
        date_to='2025-12-31 23:59:59',
        source_parallel_transfers = 2)
    # task = ERA5_T2M_SWITZERLAND(**kwargs)
    # task = ERA5_SD_SWITZERLAND(**kwargs)
    # task = ERA5_T2M_BELGIUM(**kwargs)
    # task = ERA5_SD_TAJIKISTAN(**kwargs)
    # task = ERA5_TP_TAJIKISTAN(**kwargs)

    task = ERA5_TP_SWITZERLAND(**kwargs)
    # task.retrieve()
    task.update()
    
    # era5 = ERA5_CAUCASUS_SD(download_from_origin=True, date_from='1995-01-01', source_parallel_transfers=3)
    # era5 = ERA5_BELGIUM_TP(download_from_origin=True, date_from='2021-01-01', source_parallel_transfers=2)
    # era5.retrieve_store_and_upload()
    # era5.retrieve()
    # era5.upload_to_cloud()
    # era5.store()

    # mr = MeteoRaster.load(r'C:\tethys-tasks storage test\ERA5_T2M\era5_t2m_belgium\2026\tethys_era5_t2m_2026.01.01.nct')
    # mr.plot_mean(coastline=True, borders=True)

    # files = era5.data_index['stored_file'].unique()
    # mr = MeteoRaster.load(files[-2])
    # mr.plot_mean(coastline=True, borders=True)
    # mr.get_values_from_latlon(42.5, 42.5).plot()

    # kml = r'C:\Users\zepedro\Universidade de Lisboa\IST-TETHYS - GSE training 2025.09\Shared\SHP\Rioni.kml'
    # data, centroids = mr.get_values_from_KML(kml, nameField='ID')

    # docker-compose run --rm tethys-tasks ERA5_T2M_BELGIUM update --class_kwargs "{\"download_from_origin\": \"False\", \"date_from\": \"'2026-05-01'\"}"
    # docker-compose run --rm tethys-tasks ERA5_T2M_IBERIA update --class_kwargs "{\"download_from_origin\": \"False\", \"date_from\": \"'2026-05-01'\"}"
    # docker-compose run --rm tethys-tasks ERA5_TP_SWITZERLAND update --class_kwargs "{\"download_from_origin\": \"False\", \"date_from\": \"'2026-05-01'\"}"


    pass