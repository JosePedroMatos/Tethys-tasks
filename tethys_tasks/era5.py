from tethys_tasks import BaseTask, CaptureNewVariables
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

class ERA5(BaseTask):
    '''
    Docstring for ERA5
    '''

    with CaptureNewVariables() as _ERA5_VARIABLES: #It is essential that the format of the variable here is _CLASSnAME_VARIABLES
        PUBLICATION_LATENCY = pd.Timedelta(days=5)
        PRODUCTION_FREQUENCY = pd.Timedelta(hours=1)
        LEADTIMES = pd.timedelta_range('0d', '0d', freq='1h')

        SOURCE_PARALLEL_TRANSFERS = 3

        PIXEL_SIZE = 0.1

        CLOUD_TEMPLATE = 'test/ERA5_{self._variable_upper}/era5_{self._variable}_{self._zone}/%Y/era5_{self._variable}_%Y.%m.zip'
        LOCAL_PATH_TEMPLATE = 'ERA5_{self._variable_upper}/era5_{self._variable}_{self._zone}/%Y/era5_{self._variable}_%Y.%m.zip'
        STORAGE_PATH_TEMPLATE = 'ERA5_{self._variable_upper}/era5_{self._variable}_{self._zone}/%Y/tethys_era5_{self._variable}_%Y.%m.01.nct'
        # STORAGE_PATH_TEMPLATE = 'ERA5_{self._variable_upper}/era5_{self._variable}_{self._zone}/%G/tethys_era5_{self._variable}_%G.%V.nc'
        # STORAGE_PATH_TEMPLATE = 'ERA5_{self._variable_upper}/era5_{self._variable}_{self._zone}/%Y/tethys_era5_{self._variable}_%Y.nc'

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

    def __init__(self, *args, **kwargs):

        super().__init__(*args, **kwargs)

        # store previous local file (for completeness of cumulative variables)
        index = self.populate(self.data_index['production_datetime'].min() - pd.DateOffset(years=1), self.data_index['production_datetime'].min(), silent=True)
        self.previous_local_file = index.loc[index['local_file']!=self.data_index['local_file'].iloc[0], 'local_file'].iloc[-1]

    @staticmethod
    def __download_CDS(variables):
        '''
        Downloads data from CDS
        To be used in parallel by a ThreadPool
        '''
        
        options, local_path = variables
        local_path_ = Path(local_path)

        c = cdsapi.Client()
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
                       'area': [self.source_bounding_box[d] for d in ['north', 'west', 'south', 'east']],
                       'time': [f'{h:02d}:00' for h in range(24)],
                       'nocache': ''.join(random.choice(string.digits) for _ in range(6))
                       }

            variables = ((options, local_path))
            info.append(variables)

        self.diag('        Downloading (f{self._source_parallel_transfers} threads).', 1)
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
    def _read_file(grib_file:str, variable:str='') -> dict:

        data = {}
        with xr.open_dataset(grib_file, engine='cfgrib', indexpath='') as ds:

            if variable=='':
                variable_list = list(ds.data_vars)
                if len(variable_list)>1:
                    raise Exception('The file should not have more than one data variable.')
                variable = variable_list[0]

            data['latitudes'] = ds.latitude.data
            data['longitudes'] = ds.longitude.data
            data['production_datetime'] = ds.time.data
            if isinstance(data['production_datetime'], np.datetime64):
                data['production_datetime'] = np.array([data['production_datetime']])
            data['data'] = ds[variable][...].data
            data['steps'] = ds.step.data

        if len(data['steps'])<24:
            raise Exception(f'The downloaded data does not have the expected number of time steps.')

        return data

    def _read_helper(self, grib_file:str) -> dict:
        '''
        Reads one grib file
        '''

        try:
            data = self._read_file(grib_file, self._variable)
        except Exception as ex:
            raise Exception(str(ex)[:-1] + f' ({self.__class__.__name__}).')

        if self._cumulative[self._variable] and len(data['data'].shape)!=4:
            raise Exception(f'The downloaded data does not have the expected number of dimensions {self.__class__.__name__}.')
        
        return data

    def read_local(self, local_file: str) -> MeteoRaster:
        '''
        Returns a MeteoRaster object with the ERA5 Land data
        '''
        
        self.diag(f'            Reading "{local_file}" ({self.__class__.__name__})', 1)

        with tempfile.TemporaryDirectory() as temp_dir:
            with ZipFile(local_file, 'r') as zip_ref:
                zip_ref.extractall(temp_dir)
            
            grib_files = list(Path(temp_dir).glob('*.grib'))
            parquet_files =  list(Path(temp_dir).glob('*.parquet'))
            if len(grib_files) != 1:
                raise ValueError(f'Expected exactly one .grib file in {local_file}, found {len(grib_files)}.')
            else:
                grib_file = grib_files[0]
            parquet_file = None
            if len(parquet_files) ==1:
                parquet_file = parquet_files[0]
            elif len(parquet_files)>=1:
                raise ValueError(f'Expected exactly one .parquet file in {local_file}, found {len(parquet_files)}.')
            
            data = self._read_helper(grib_file)

            if parquet_file is None:
                parquet_file_ = Path(local_file).with_suffix('.parquet')
                if parquet_file_.exists():
                    parquet_file = parquet_file_

            if parquet_file:
                last_cum_step = pd.read_parquet(parquet_file)
                if last_cum_step.shape != data['data'].shape[-2:]:
                    raise Exception(f'ERA5 Land ({self._variable}) processing failed. Lat and Lon of downloaded and stored files do not match.')
                data['data'][-1, -1, ...] = last_cum_step.values

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

                    prev_cum_step = pd.DataFrame(data['data'][0, -1, :, :],
                                                index=pd.Index(data['latitudes'], name='latitude'),
                                                columns=pd.Index(data['longitudes'], name='longitude'),
                                                )
                
                    prev_cum_step.to_parquet(parquet_path)
                except Exception as ex:
                    print(f'Creation of .parquet file failed ({parquet_path}) ({self.__class__.__name__}).')

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
        data['leadtimes'] = np.array([pd.Timedelta('0d')])
                
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

            complete = self.data_index.groupby('local_file').all().loc[:, 'data_exists']

            for f0 in complete.index[complete].tolist():
                # All the data is available. Check .parquet

                with ZipFile(f0, mode='a') as zip_file:
                    names = zip_file.namelist()
                    local_parquet = Path(f0).with_suffix('.parquet')
                    if len(names)!=2:
                        if not local_parquet.exists():
                            # Try to create the local .parquet by reading the next localfile
                            reference_datetime = self.data_index.loc[self.data_index['local_file']==f0, 'production_datetime'].max()
                            index = self.populate(reference_datetime, reference_datetime + pd.DateOffset(years=1))
                            next_local_file = index.loc[index['local_file']!=f0, 'local_file'].iloc[0]
                            if Path(next_local_file).exists():
                                self.read_local(next_local_file)

                        if local_parquet.exists():
                            try:
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
                    complete = False        
                    with ZipFile(f0, mode='a') as zip_file:
                        names = zip_file.namelist()
                        extensions = [ext.split('.')[-1] for ext in names]

                        if len(extensions)>2:
                            raise(f'Problem with the file ({Path(f0).absolute()}) ({self.__class__.__name__}).')
                        elif len(extensions)<2:
                            self.data_index.loc[self.data_index['local_file']==f0, 'local_file_complete'] = False

            self._update_completeness(stored=False)

class ERA5_ZAMBEZI_T2M(ERA5):

    with CaptureNewVariables() as _ERA5_ZAMBEZI_T2M_VARIABLES: #It is essential that the format of the variable here is _CLASSNAME_VARIABLES
        SOURCE_KML='tethys_tasks/resources/zambezi.kml'
        VARIABLE='t2m'
        ZONE='zambezi'

class ERA5_ZAMBEZI_TP(ERA5_ZAMBEZI_T2M):

    with CaptureNewVariables() as _ERA5_ZAMBEZI_TP_VARIABLES: #It is essential that the format of the variable here is _CLASSNAME_VARIABLES
        VARIABLE='tp'

class ERA5_BELGIUM_T2M(ERA5):

    with CaptureNewVariables() as _ERA5_BELGIUM_T2M_VARIABLES: #It is essential that the format of the variable here is _CLASSNAME_VARIABLES
        SOURCE_KML='tethys_tasks/resources/belgium.kml'
        VARIABLE='t2m'
        ZONE='belgium'

class ERA5_BELGIUM_TP(ERA5_BELGIUM_T2M):

    with CaptureNewVariables() as _ERA5_BELGIUM_TP_VARIABLES: #It is essential that the format of the variable here is _CLASSNAME_VARIABLES
        VARIABLE='tp'

class ERA5_CAUCASUS_T2M(ERA5):

    with CaptureNewVariables() as _ERA5_CAUCASUS_T2M_VARIABLES: #It is essential that the format of the variable here is _CLASSNAME_VARIABLES
        SOURCE_KML='tethys_tasks/resources/caucasus.kml'
        VARIABLE='t2m'
        ZONE='caucasus'

class ERA5_CAUCASUS_TP(ERA5_CAUCASUS_T2M):

    with CaptureNewVariables() as _ERA5_CAUCASUS_TP_VARIABLES: #It is essential that the format of the variable here is _CLASSNAME_VARIABLES
        VARIABLE='tp'

class ERA5_IBERIA_T2M(ERA5):

    with CaptureNewVariables() as _ERA5_IBERIA_T2M_VARIABLES: #It is essential that the format of the variable here is _CLASSNAME_VARIABLES
        SOURCE_KML='tethys_tasks/resources/iberia.kml'
        VARIABLE='t2m'
        ZONE='iberia'

class ERA5_IBERIA_TP(ERA5_IBERIA_T2M):

    with CaptureNewVariables() as _ERA5_IBERIA_TP_VARIABLES: #It is essential that the format of the variable here is _CLASSNAME_VARIABLES
        VARIABLE='tp'

class ERA5_TAJIKISTAN_T2M(ERA5):

    with CaptureNewVariables() as _ERA5_TAJIKISTAN_T2M_VARIABLES: #It is essential that the format of the variable here is _CLASSNAME_VARIABLES
        SOURCE_KML='tethys_tasks/resources/tajikistan.kml'
        VARIABLE='t2m'
        ZONE='tajikistan'

class ERA5_TAJIKISTAN_TP(ERA5_TAJIKISTAN_T2M):

    with CaptureNewVariables() as _ERA5_TAJIKISTAN_TP_VARIABLES: #It is essential that the format of the variable here is _CLASSNAME_VARIABLES
        VARIABLE='tp'



if __name__=='__main__':
    import matplotlib.pyplot as plt
    plt.ion()

    era5 = ERA5_BELGIUM_T2M(download_from_source=True, date_from='2025-10-01', source_parallel_transfers=2)
    # era5 = ERA5_BELGIUM_TP(download_from_source=True, date_from='2021-01-01', source_parallel_transfers=2)
    era5.retrieve_and_upload(verify=False)
    # era5.retrieve()
    # era5.upload_to_cloud()
    era5.store()

    # mr = MeteoRaster.load(r'C:\tethys-tasks storage test\ERA5_T2M\era5_t2m_belgium\2026\tethys_era5_t2m_2026.01.01.nct')
    # mr.plot_mean(coastline=True, borders=True)
    pass