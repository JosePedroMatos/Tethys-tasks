from tethys_tasks import BaseTask, CaptureNewVariables
import pandas as pd
import xarray as xr
from pathlib import Path
import cdsapi
import shutil
import tempfile
from meteoraster import MeteoRaster
import numpy as np
from concurrent.futures import ThreadPoolExecutor, as_completed
import random
import string

def _leadtimes_and_month_seconds(valid_time, reference_date):
    '''
    C3S monthly means average the calendar month ENDING at valid_time: a January run's
    first field carries valid_time = 1 Feb and holds the January mean (CDS
    leadtime_month=1 is the initialisation month itself).

    Returns the 0-based DateOffset leadtimes of the averaged months and their length
    in seconds -- the factor that turns a mean rate (m/s) into m/month.
    '''

    averaged = pd.DatetimeIndex(valid_time) - pd.DateOffset(months=1)
    reference_date = pd.Timestamp(reference_date)
    leadtimes = pd.Index([pd.DateOffset(months=(d.year - reference_date.year) * 12
                                               + d.month - reference_date.month)
                          for d in averaged])

    return leadtimes, averaged.days_in_month.to_numpy(dtype=float) * 86400

class C3S_ECMWF51_T2M_WORLD(BaseTask):
    '''
    Docstring for C3S_ECMWF
    https://cds.climate.copernicus.eu/datasets/seasonal-monthly-single-levels?tab=overview

    Two properties of these files drive the readers below:
    * tprate is a mean rate in m/s ("Mean total precipitation rate"), not an accumulation.
    * valid_time is the END of the averaged calendar month, so a January run's first
      field (CDS leadtime_month=1) is the January mean and carries valid_time = 1 Feb.
      Leadtimes are therefore 0-based: leadtime 0 is the month starting at
      production_datetime, matching how ERA5/ERA5M label an accumulation period.
    '''

    with CaptureNewVariables() as _C3S_ECMWF51_T2M_WORLD_VARIABLES: #It is essential that the format of the variable here is _CLASSnAME_VARIABLES
        PUBLICATION_LATENCY = pd.Timedelta(days=13)
        PRODUCTION_FREQUENCY = pd.DateOffset(months=1)
        FAIL_IF_OLDER = pd.Timedelta(days=45)
                
        LEADTIME_MONTH = ['1', '2', '3', '4', '5', '6']   # CDS request: 1 is the initialisation month
        LEADTIMES = [pd.DateOffset(months=int(m)-1) for m in LEADTIME_MONTH]

        SOURCE_PARALLEL_TRANSFERS = 1

        C3S_SYSTEM = '51'
        ORIGINATING_CENTRE = 'ecmwf'
        MISSING_YEARS = [i for i in range(1970, 1981)]
        PIXEL_SIZE = 1
        VARIABLE='t2m'
        ZONE='world'

        ASSUME_LOCAL_COMPLETE = True

        CLOUD_TEMPLATE = f'C3S/C3S_{ORIGINATING_CENTRE.upper()}{C3S_SYSTEM}_{{self._variable_upper}}/c3s_{ORIGINATING_CENTRE.lower()}{C3S_SYSTEM}_{{self._variable}}_{{self._zone}}/%Y/c3s_{ORIGINATING_CENTRE.lower()}{C3S_SYSTEM}_{{self._variable}}_%Y.%m.grib'
        LOCAL_PATH_TEMPLATE = f'C3S/C3S_{ORIGINATING_CENTRE.upper()}{C3S_SYSTEM}_{{self._variable_upper}}/c3s_{ORIGINATING_CENTRE.lower()}{C3S_SYSTEM}_{{self._variable}}_{{self._zone}}/%Y/c3s_{ORIGINATING_CENTRE.lower()}{C3S_SYSTEM}_{{self._variable}}_%Y.%m.grib'
        STORAGE_PATH_TEMPLATE = f'C3S/C3S_{ORIGINATING_CENTRE.upper()}{C3S_SYSTEM}_{{self._variable_upper}}/c3s_{ORIGINATING_CENTRE.lower()}{C3S_SYSTEM}_{{self._variable}}_{{self._zone}}/%Y/tethys_c3s_{ORIGINATING_CENTRE.lower()}{C3S_SYSTEM}_{{self._variable}}_%Y.%m.nct'

        STORAGE_SEARCH_WINDOW = pd.DateOffset(days=40)

        VARIABLE_DICT = dict(
            t2m = '2m_temperature',
            tprate = 'total_precipitation',
        )

        CUMULATIVE = dict(
            tprate=True,
            t2m=False,
        )

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
            c.retrieve('seasonal-monthly-single-levels', options).download(temp_file_path)
            if local_path_.exists():
                local_path_.unlink()
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
            
            if date.year in self._missing_years:
                self.diag(f'        Skipping due to missing year: {date.strftime("%Y-%m")} ({self.__class__.__name__}).', 1)
                continue
            
            options = {'data_format': 'grib',
                       'year': [f'{date.year}'],
                       'month': [f'{date.month:02d}'],
                       'originating_centre': self._originating_centre,
                       'system': self._c3s_system,
                       'variable': [self._variable_dict[self._variable]],
                       'area': [self.source_bounding_box[d] for d in ['north', 'west', 'south', 'east']],
                       'leadtime_month': self._leadtime_month,
                       'product_type': ['monthly_mean'],
                       'nocache': ''.join(random.choice(string.digits) for _ in range(6))
                       }

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
                    self.diag(f'            Downloaded "{local_path_}" ({self.__class__.__name__})', 1)
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
            if isinstance(data['production_datetime'], np.datetime64) or data['production_datetime'].ndim==0:
                data['production_datetime'] = np.array([data['production_datetime'],])
            # This reader assumes one initialisation per file, i.e. dims (number, step,
            # lat, lon). A centre that switches to a lagged ensemble gains a time
            # dimension and needs the C3S_UKMO610_T2M_WORLD reader instead.
            if np.atleast_1d(ds.valid_time.data).ndim!=1:
                raise Exception(f'"{Path(grib_file).name}" holds {len(data["production_datetime"])} '
                                'initialisations; this reader expects one.')
            data['data'] = np.expand_dims(ds[variable][...].data, axis=0)
            data['leadtimes'], seconds = _leadtimes_and_month_seconds(np.atleast_1d(ds.valid_time.data),
                                                                     data['production_datetime'][0])

        if variable=='tprate':
            # Mean rate (m/s) -> m/month; read_local applies the m -> mm factor.
            data['data'] *= seconds[None, None, :, None, None]

        return data

    def _read_helper(self, grib_file:str) -> dict:
        '''
        Reads one grib file
        '''

        try:
            data = self._read_file(grib_file, self._variable)
        except Exception as ex:
            raise Exception(str(ex)[:-1] + f' ({self.__class__.__name__}).')
        
        return data

    def read_local(self, local_file: str) -> MeteoRaster:
        '''
        Returns a MeteoRaster object with the C3S seasonal forecast data
        '''
        
        self.diag(f'            Reading "{local_file}" ({self.__class__.__name__})', 1)

        data = self._read_helper(local_file)
                
        if self._variable == 'tprate':
            data['data'] *= 1000
            units = 'mm/month'
        elif self._variable == 't2m':
            data['data'] -= 273.15
            units = 'C'
        else:
            units = 'unknown'
                
        data['Production_datetime'] = pd.DatetimeIndex(data['production_datetime'])

        tmp = MeteoRaster(data, units=units, variable=self._variable, verbose=False)
        tmp.trim()
        # tmp.getDataFromLatLon(26, -14).to_clipboard(excel=True)
        # tmp.plot_mean(multiplier=12, vmax=3000)
        # tmp.get_cropped(from_lon=-170, to_lon=170, from_lat=-85, to_lat=85).plot_mean(multiplier=12, vmax=3000, borders=True)
        # a = tmp.get_cropped(from_lon=-170, to_lon=170, from_lat=-85, to_lat=85)
        # a.longitudes +=

        return tmp

class C3S_ECMWF51_TPRATE_WORLD(C3S_ECMWF51_T2M_WORLD):
    with CaptureNewVariables() as _C3S_ECMWF51_TPRATE_WORLD_VARIABLES: #It is essential that the format of the variable here is _CLASSNAME_VARIABLES
        VARIABLE='tprate'
        ZONE='world'

class C3S_UKMO610_T2M_WORLD(C3S_ECMWF51_T2M_WORLD):
    with CaptureNewVariables() as _C3S_UKMO610_T2M_WORLD_VARIABLES: #It is essential that the format of the variable here is _CLASSNAME_VARIABLES
        VARIABLE='t2m'
        ZONE='world'

        C3S_SYSTEM = '610'
        ORIGINATING_CENTRE = 'ukmo'
        MISSING_YEARS = [i for i in range(1970, 1993)] + [i for i in range(2017, 2026)] 

        CLOUD_TEMPLATE = f'C3S/C3S_{ORIGINATING_CENTRE.upper()}{C3S_SYSTEM}_{{self._variable_upper}}/c3s_{ORIGINATING_CENTRE.lower()}{C3S_SYSTEM}_{{self._variable}}_{{self._zone}}/%Y/c3s_{ORIGINATING_CENTRE.lower()}{C3S_SYSTEM}_{{self._variable}}_%Y.%m.grib'
        LOCAL_PATH_TEMPLATE = f'C3S/C3S_{ORIGINATING_CENTRE.upper()}{C3S_SYSTEM}_{{self._variable_upper}}/c3s_{ORIGINATING_CENTRE.lower()}{C3S_SYSTEM}_{{self._variable}}_{{self._zone}}/%Y/c3s_{ORIGINATING_CENTRE.lower()}{C3S_SYSTEM}_{{self._variable}}_%Y.%m.grib'
        STORAGE_PATH_TEMPLATE = f'C3S/C3S_{ORIGINATING_CENTRE.upper()}{C3S_SYSTEM}_{{self._variable_upper}}/c3s_{ORIGINATING_CENTRE.lower()}{C3S_SYSTEM}_{{self._variable}}_{{self._zone}}/%Y/tethys_c3s_{ORIGINATING_CENTRE.lower()}{C3S_SYSTEM}_{{self._variable}}_%Y.%m.nct'

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

            valid_time = pd.DataFrame(ds.valid_time.data)
            valid_time.index.name = 'time'
            valid_time.columns.name = 'step'

            # reference_date = pd.Timestamp(ds.time.data[-1]).normalize()
            reference_date = pd.Timestamp(f'{"-".join(grib_file.split("_")[-1].replace(".grib", "").split("."))}')
            # event_dates are the GRIB valid_times (the end of each averaged month); they
            # select the steps to read. The leadtimes label the averaged month itself.
            event_dates = pd.DatetimeIndex([reference_date + pd.DateOffset(months=int(m)) for m in range(1, 7)])
            data['production_datetime'] = pd.DatetimeIndex([reference_date])
            data['leadtimes'], seconds = _leadtimes_and_month_seconds(event_dates, reference_date)

            # cfgrib merges the daily initialisations of the lagged ensemble into a
            # (number, time) hypercube in which only the members belonging to an
            # initialisation are set, the rest being NaN. Which members those are is
            # probed on a single pixel rather than computed: the member count per day is
            # not always constant (ukmo604 2025.11 has a day with 1 and a day with 3),
            # so any fixed-stride arithmetic silently drops members and leaves NaN ones.
            # Members are concatenated oldest initialisation first.
            blocks = []
            for time_idx in range(ds[variable].shape[1]):
                idxs = np.where(valid_time.query('time==@time_idx').iloc[0].dt.normalize().isin(event_dates.normalize()))[0]
                if len(idxs)!=len(event_dates):
                    raise Exception(f'Initialisation {time_idx} covers {len(idxs)} of the {len(event_dates)} averaged months.')
                owned = np.where(np.isfinite(ds[variable][:, time_idx, idxs[0], 0, 0].data))[0]
                blocks.append(ds[variable][owned, time_idx, idxs, ...].data)

            assigned = sum(b.shape[0] for b in blocks)
            if assigned!=ds[variable].shape[0]:
                raise Exception(f'Only {assigned} of the {ds[variable].shape[0]} members were assigned to an initialisation.')

            data['data'] = np.concatenate(blocks)[None, ...]

        if variable in ['tprate']:
            # Mean rate (m/s) -> m/month; read_local applies the m -> mm factor.
            data['data'] *= seconds[None, None, :, None, None]

        return data

class C3S_UKMO610_TPRATE_WORLD(C3S_UKMO610_T2M_WORLD):
    with CaptureNewVariables() as _C3S_UKMO610_TPRATE_WORLD_VARIABLES: #It is essential that the format of the variable here is _CLASSNAME_VARIABLES
        VARIABLE='tprate'

class C3S_UKMO604_T2M_WORLD(C3S_UKMO610_T2M_WORLD):
    with CaptureNewVariables() as _C3S_UKMO604_T2M_WORLD_VARIABLES: #It is essential that the format of the variable here is _CLASSNAME_VARIABLES
        VARIABLE='t2m'
        ZONE='world'

        C3S_SYSTEM = '604'
        ORIGINATING_CENTRE = 'ukmo'
        MISSING_YEARS = [i for i in range(1970, 1993)] + [i for i in range(2017, 2025)] 

        CLOUD_TEMPLATE = f'C3S/C3S_{ORIGINATING_CENTRE.upper()}{C3S_SYSTEM}_{{self._variable_upper}}/c3s_{ORIGINATING_CENTRE.lower()}{C3S_SYSTEM}_{{self._variable}}_{{self._zone}}/%Y/c3s_{ORIGINATING_CENTRE.lower()}{C3S_SYSTEM}_{{self._variable}}_%Y.%m.grib'
        LOCAL_PATH_TEMPLATE = f'C3S/C3S_{ORIGINATING_CENTRE.upper()}{C3S_SYSTEM}_{{self._variable_upper}}/c3s_{ORIGINATING_CENTRE.lower()}{C3S_SYSTEM}_{{self._variable}}_{{self._zone}}/%Y/c3s_{ORIGINATING_CENTRE.lower()}{C3S_SYSTEM}_{{self._variable}}_%Y.%m.grib'
        STORAGE_PATH_TEMPLATE = f'C3S/C3S_{ORIGINATING_CENTRE.upper()}{C3S_SYSTEM}_{{self._variable_upper}}/c3s_{ORIGINATING_CENTRE.lower()}{C3S_SYSTEM}_{{self._variable}}_{{self._zone}}/%Y/tethys_c3s_{ORIGINATING_CENTRE.lower()}{C3S_SYSTEM}_{{self._variable}}_%Y.%m.nct'

class C3S_UKMO604_TPRATE_WORLD(C3S_UKMO604_T2M_WORLD):
    with CaptureNewVariables() as _C3S_UKMO604_TPRATE_WORLD_VARIABLES: #It is essential that the format of the variable here is _CLASSNAME_VARIABLES
        VARIABLE='tprate'

class C3S_MF9_T2M_WORLD(C3S_ECMWF51_T2M_WORLD):
    with CaptureNewVariables() as _C3S_MF9_T2M_WORLD_VARIABLES: #It is essential that the format of the variable here is _CLASSNAME_VARIABLES
        VARIABLE='t2m'
        ZONE='world'

        C3S_SYSTEM = '9'
        ORIGINATING_CENTRE = 'meteo_france'
        MISSING_YEARS = [i for i in range(1970, 1993)]

        CLOUD_TEMPLATE = f'C3S/C3S_{ORIGINATING_CENTRE.upper()}{C3S_SYSTEM}_{{self._variable_upper}}/c3s_{ORIGINATING_CENTRE.lower()}{C3S_SYSTEM}_{{self._variable}}_{{self._zone}}/%Y/c3s_{ORIGINATING_CENTRE.lower()}{C3S_SYSTEM}_{{self._variable}}_%Y.%m.grib'
        LOCAL_PATH_TEMPLATE = f'C3S/C3S_{ORIGINATING_CENTRE.upper()}{C3S_SYSTEM}_{{self._variable_upper}}/c3s_{ORIGINATING_CENTRE.lower()}{C3S_SYSTEM}_{{self._variable}}_{{self._zone}}/%Y/c3s_{ORIGINATING_CENTRE.lower()}{C3S_SYSTEM}_{{self._variable}}_%Y.%m.grib'
        STORAGE_PATH_TEMPLATE = f'C3S/C3S_{ORIGINATING_CENTRE.upper()}{C3S_SYSTEM}_{{self._variable_upper}}/c3s_{ORIGINATING_CENTRE.lower()}{C3S_SYSTEM}_{{self._variable}}_{{self._zone}}/%Y/tethys_c3s_{ORIGINATING_CENTRE.lower()}{C3S_SYSTEM}_{{self._variable}}_%Y.%m.nct'

class C3S_MF9_TPRATE_WORLD(C3S_MF9_T2M_WORLD):
    with CaptureNewVariables() as _C3S_MF9_TPRATE_WORLD_VARIABLES: #It is essential that the format of the variable here is _CLASSNAME_VARIABLES
        VARIABLE='tprate'

class C3S_DWD22_T2M_WORLD(C3S_ECMWF51_T2M_WORLD):
    with CaptureNewVariables() as _C3S_DWD22_T2M_WORLD_VARIABLES: #It is essential that the format of the variable here is _CLASSNAME_VARIABLES
        VARIABLE='t2m'
        ZONE='world'

        C3S_SYSTEM = '22'
        ORIGINATING_CENTRE = 'dwd'
        MISSING_YEARS = [i for i in range(1970, 1993)] + [i for i in range(2024, 2025)] 

        CLOUD_TEMPLATE = f'C3S/C3S_{ORIGINATING_CENTRE.upper()}{C3S_SYSTEM}_{{self._variable_upper}}/c3s_{ORIGINATING_CENTRE.lower()}{C3S_SYSTEM}_{{self._variable}}_{{self._zone}}/%Y/c3s_{ORIGINATING_CENTRE.lower()}{C3S_SYSTEM}_{{self._variable}}_%Y.%m.grib'
        LOCAL_PATH_TEMPLATE = f'C3S/C3S_{ORIGINATING_CENTRE.upper()}{C3S_SYSTEM}_{{self._variable_upper}}/c3s_{ORIGINATING_CENTRE.lower()}{C3S_SYSTEM}_{{self._variable}}_{{self._zone}}/%Y/c3s_{ORIGINATING_CENTRE.lower()}{C3S_SYSTEM}_{{self._variable}}_%Y.%m.grib'
        STORAGE_PATH_TEMPLATE = f'C3S/C3S_{ORIGINATING_CENTRE.upper()}{C3S_SYSTEM}_{{self._variable_upper}}/c3s_{ORIGINATING_CENTRE.lower()}{C3S_SYSTEM}_{{self._variable}}_{{self._zone}}/%Y/tethys_c3s_{ORIGINATING_CENTRE.lower()}{C3S_SYSTEM}_{{self._variable}}_%Y.%m.nct'

class C3S_DWD22_TPRATE_WORLD(C3S_DWD22_T2M_WORLD):
    with CaptureNewVariables() as _C3S_DWD22_TPRATE_WORLD_VARIABLES: #It is essential that the format of the variable here is _CLASSNAME_VARIABLES
        VARIABLE='tprate'

class C3S_CMCC4_T2M_WORLD(C3S_ECMWF51_T2M_WORLD):
    with CaptureNewVariables() as _C3S_CMCC4_T2M_WORLD_VARIABLES: #It is essential that the format of the variable here is _CLASSNAME_VARIABLES
        VARIABLE='t2m'
        ZONE='world'

        C3S_SYSTEM = '4'
        ORIGINATING_CENTRE = 'cmcc'
        MISSING_YEARS = [] 

        CLOUD_TEMPLATE = f'C3S/C3S_{ORIGINATING_CENTRE.upper()}{C3S_SYSTEM}_{{self._variable_upper}}/c3s_{ORIGINATING_CENTRE.lower()}{C3S_SYSTEM}_{{self._variable}}_{{self._zone}}/%Y/c3s_{ORIGINATING_CENTRE.lower()}{C3S_SYSTEM}_{{self._variable}}_%Y.%m.grib'
        LOCAL_PATH_TEMPLATE = f'C3S/C3S_{ORIGINATING_CENTRE.upper()}{C3S_SYSTEM}_{{self._variable_upper}}/c3s_{ORIGINATING_CENTRE.lower()}{C3S_SYSTEM}_{{self._variable}}_{{self._zone}}/%Y/c3s_{ORIGINATING_CENTRE.lower()}{C3S_SYSTEM}_{{self._variable}}_%Y.%m.grib'
        STORAGE_PATH_TEMPLATE = f'C3S/C3S_{ORIGINATING_CENTRE.upper()}{C3S_SYSTEM}_{{self._variable_upper}}/c3s_{ORIGINATING_CENTRE.lower()}{C3S_SYSTEM}_{{self._variable}}_{{self._zone}}/%Y/tethys_c3s_{ORIGINATING_CENTRE.lower()}{C3S_SYSTEM}_{{self._variable}}_%Y.%m.nct'

class C3S_CMCC4_TPRATE_WORLD(C3S_CMCC4_T2M_WORLD):
    with CaptureNewVariables() as _C3S_CMCC4_TPRATE_WORLD_VARIABLES: #It is essential that the format of the variable here is _CLASSNAME_VARIABLES
        VARIABLE='tprate'

class C3S_NCEP2_T2M_WORLD(C3S_UKMO610_T2M_WORLD):
    with CaptureNewVariables() as _C3S_NCEP2_T2M_WORLD_VARIABLES: #It is essential that the format of the variable here is _CLASSNAME_VARIABLES
        VARIABLE='t2m'
        ZONE='world'

        C3S_SYSTEM = '2'
        ORIGINATING_CENTRE = 'ncep'
        MISSING_YEARS = [i for i in range(1970, 1993)] + [i for i in range(2017, 2019)] 

        CLOUD_TEMPLATE = f'C3S/C3S_{ORIGINATING_CENTRE.upper()}{C3S_SYSTEM}_{{self._variable_upper}}/c3s_{ORIGINATING_CENTRE.lower()}{C3S_SYSTEM}_{{self._variable}}_{{self._zone}}/%Y/c3s_{ORIGINATING_CENTRE.lower()}{C3S_SYSTEM}_{{self._variable}}_%Y.%m.grib'
        LOCAL_PATH_TEMPLATE = f'C3S/C3S_{ORIGINATING_CENTRE.upper()}{C3S_SYSTEM}_{{self._variable_upper}}/c3s_{ORIGINATING_CENTRE.lower()}{C3S_SYSTEM}_{{self._variable}}_{{self._zone}}/%Y/c3s_{ORIGINATING_CENTRE.lower()}{C3S_SYSTEM}_{{self._variable}}_%Y.%m.grib'
        STORAGE_PATH_TEMPLATE = f'C3S/C3S_{ORIGINATING_CENTRE.upper()}{C3S_SYSTEM}_{{self._variable_upper}}/c3s_{ORIGINATING_CENTRE.lower()}{C3S_SYSTEM}_{{self._variable}}_{{self._zone}}/%Y/tethys_c3s_{ORIGINATING_CENTRE.lower()}{C3S_SYSTEM}_{{self._variable}}_%Y.%m.nct'

class C3S_NCEP2_TPRATE_WORLD(C3S_NCEP2_T2M_WORLD):
    with CaptureNewVariables() as _C3S_NCEP2_TPRATE_WORLD_VARIABLES: #It is essential that the format of the variable here is _CLASSNAME_VARIABLES
        VARIABLE='tprate'

class C3S_JMA4_T2M_WORLD(C3S_UKMO610_T2M_WORLD):
    with CaptureNewVariables() as _C3S_JMA4_T2M_WORLD_VARIABLES: #It is essential that the format of the variable here is _CLASSNAME_VARIABLES
        VARIABLE='t2m'
        ZONE='world'

        C3S_SYSTEM = '4'
        ORIGINATING_CENTRE = 'jma'
        MISSING_YEARS = [i for i in range(1970, 1993)]

        CLOUD_TEMPLATE = f'C3S/C3S_{ORIGINATING_CENTRE.upper()}{C3S_SYSTEM}_{{self._variable_upper}}/c3s_{ORIGINATING_CENTRE.lower()}{C3S_SYSTEM}_{{self._variable}}_{{self._zone}}/%Y/c3s_{ORIGINATING_CENTRE.lower()}{C3S_SYSTEM}_{{self._variable}}_%Y.%m.grib'
        LOCAL_PATH_TEMPLATE = f'C3S/C3S_{ORIGINATING_CENTRE.upper()}{C3S_SYSTEM}_{{self._variable_upper}}/c3s_{ORIGINATING_CENTRE.lower()}{C3S_SYSTEM}_{{self._variable}}_{{self._zone}}/%Y/c3s_{ORIGINATING_CENTRE.lower()}{C3S_SYSTEM}_{{self._variable}}_%Y.%m.grib'
        STORAGE_PATH_TEMPLATE = f'C3S/C3S_{ORIGINATING_CENTRE.upper()}{C3S_SYSTEM}_{{self._variable_upper}}/c3s_{ORIGINATING_CENTRE.lower()}{C3S_SYSTEM}_{{self._variable}}_{{self._zone}}/%Y/tethys_c3s_{ORIGINATING_CENTRE.lower()}{C3S_SYSTEM}_{{self._variable}}_%Y.%m.nct'

class C3S_JMA4_TPRATE_WORLD(C3S_JMA4_T2M_WORLD):
    with CaptureNewVariables() as _C3S_JMA4_TPRATE_WORLD_VARIABLES: #It is essential that the format of the variable here is _CLASSNAME_VARIABLES
        VARIABLE='tprate'

class C3S_JMA3_T2M_WORLD(C3S_UKMO610_T2M_WORLD):
    with CaptureNewVariables() as _C3S_JMA3_T2M_WORLD_VARIABLES: #It is essential that the format of the variable here is _CLASSNAME_VARIABLES
        C3S_SYSTEM = '3'
        ORIGINATING_CENTRE = 'jma'

        MISSING_YEARS = [i for i in range(1970, 1993)]

        CLOUD_TEMPLATE = f'C3S/C3S_{ORIGINATING_CENTRE.upper()}{C3S_SYSTEM}_{{self._variable_upper}}/c3s_{ORIGINATING_CENTRE.lower()}{C3S_SYSTEM}_{{self._variable}}_{{self._zone}}/%Y/c3s_{ORIGINATING_CENTRE.lower()}{C3S_SYSTEM}_{{self._variable}}_%Y.%m.grib'
        LOCAL_PATH_TEMPLATE = f'C3S/C3S_{ORIGINATING_CENTRE.upper()}{C3S_SYSTEM}_{{self._variable_upper}}/c3s_{ORIGINATING_CENTRE.lower()}{C3S_SYSTEM}_{{self._variable}}_{{self._zone}}/%Y/c3s_{ORIGINATING_CENTRE.lower()}{C3S_SYSTEM}_{{self._variable}}_%Y.%m.grib'
        STORAGE_PATH_TEMPLATE = f'C3S/C3S_{ORIGINATING_CENTRE.upper()}{C3S_SYSTEM}_{{self._variable_upper}}/c3s_{ORIGINATING_CENTRE.lower()}{C3S_SYSTEM}_{{self._variable}}_{{self._zone}}/%Y/tethys_c3s_{ORIGINATING_CENTRE.lower()}{C3S_SYSTEM}_{{self._variable}}_%Y.%m.nct'

class C3S_JMA3_TPRATE_WORLD(C3S_JMA3_T2M_WORLD):
    with CaptureNewVariables() as _C3S_JMA3_TPRATE_WORLD_VARIABLES: #It is essential that the format of the variable here is _CLASSNAME_VARIABLES
        VARIABLE='tprate'

class C3S_ECCC5_T2M_WORLD(C3S_ECMWF51_T2M_WORLD):
    with CaptureNewVariables() as _C3S_ECCC5_T2M_WORLD_VARIABLES: #It is essential that the format of the variable here is _CLASSNAME_VARIABLES
        VARIABLE='t2m'
        ZONE='world'

        C3S_SYSTEM = '5'
        ORIGINATING_CENTRE = 'eccc'
        MISSING_YEARS = [i for i in range(1970, 1993)]

        CLOUD_TEMPLATE = f'C3S/C3S_{ORIGINATING_CENTRE.upper()}{C3S_SYSTEM}_{{self._variable_upper}}/c3s_{ORIGINATING_CENTRE.lower()}{C3S_SYSTEM}_{{self._variable}}_{{self._zone}}/%Y/c3s_{ORIGINATING_CENTRE.lower()}{C3S_SYSTEM}_{{self._variable}}_%Y.%m.grib'
        LOCAL_PATH_TEMPLATE = f'C3S/C3S_{ORIGINATING_CENTRE.upper()}{C3S_SYSTEM}_{{self._variable_upper}}/c3s_{ORIGINATING_CENTRE.lower()}{C3S_SYSTEM}_{{self._variable}}_{{self._zone}}/%Y/c3s_{ORIGINATING_CENTRE.lower()}{C3S_SYSTEM}_{{self._variable}}_%Y.%m.grib'
        STORAGE_PATH_TEMPLATE = f'C3S/C3S_{ORIGINATING_CENTRE.upper()}{C3S_SYSTEM}_{{self._variable_upper}}/c3s_{ORIGINATING_CENTRE.lower()}{C3S_SYSTEM}_{{self._variable}}_{{self._zone}}/%Y/tethys_c3s_{ORIGINATING_CENTRE.lower()}{C3S_SYSTEM}_{{self._variable}}_%Y.%m.nct'

class C3S_ECCC5_TPRATE_WORLD(C3S_ECCC5_T2M_WORLD):
    with CaptureNewVariables() as _C3S_ECCC5_TPRATE_WORLD_VARIABLES: #It is essential that the format of the variable here is _CLASSNAME_VARIABLES
        VARIABLE='tprate'

        MISSING_YEARS = [i for i in range(1970, 1993)] + [2024]

class C3S_BOM2_T2M_WORLD(C3S_UKMO610_T2M_WORLD):
    with CaptureNewVariables() as _C3S_BOM2_T2M_WORLD_VARIABLES: #It is essential that the format of the variable here is _CLASSNAME_VARIABLES
        VARIABLE='t2m'
        ZONE='world'

        C3S_SYSTEM = '2'
        ORIGINATING_CENTRE = 'bom'
        MISSING_YEARS = [i for i in range(1970, 1993)] + [i for i in range(2019, 2025)] 

        CLOUD_TEMPLATE = f'C3S/C3S_{ORIGINATING_CENTRE.upper()}{C3S_SYSTEM}_{{self._variable_upper}}/c3s_{ORIGINATING_CENTRE.lower()}{C3S_SYSTEM}_{{self._variable}}_{{self._zone}}/%Y/c3s_{ORIGINATING_CENTRE.lower()}{C3S_SYSTEM}_{{self._variable}}_%Y.%m.grib'
        LOCAL_PATH_TEMPLATE = f'C3S/C3S_{ORIGINATING_CENTRE.upper()}{C3S_SYSTEM}_{{self._variable_upper}}/c3s_{ORIGINATING_CENTRE.lower()}{C3S_SYSTEM}_{{self._variable}}_{{self._zone}}/%Y/c3s_{ORIGINATING_CENTRE.lower()}{C3S_SYSTEM}_{{self._variable}}_%Y.%m.grib'
        STORAGE_PATH_TEMPLATE = f'C3S/C3S_{ORIGINATING_CENTRE.upper()}{C3S_SYSTEM}_{{self._variable_upper}}/c3s_{ORIGINATING_CENTRE.lower()}{C3S_SYSTEM}_{{self._variable}}_{{self._zone}}/%Y/tethys_c3s_{ORIGINATING_CENTRE.lower()}{C3S_SYSTEM}_{{self._variable}}_%Y.%m.nct'

class C3S_BOM2_TPRATE_WORLD(C3S_BOM2_T2M_WORLD):
    with CaptureNewVariables() as _C3S_BOM2_TPRATE_WORLD_VARIABLES: #It is essential that the format of the variable here is _CLASSNAME_VARIABLES
        VARIABLE='tprate'

if __name__=='__main__':
    import matplotlib.pyplot as plt
    plt.ion()


    classes = [
        C3S_ECMWF51_T2M_WORLD,
        C3S_ECMWF51_TPRATE_WORLD,
        # C3S_UKMO610_T2M_WORLD,
        # C3S_UKMO610_TPRATE_WORLD,
        # C3S_MF9_T2M_WORLD,
        # C3S_MF9_TPRATE_WORLD,
        # C3S_DWD22_T2M_WORLD,
        # C3S_DWD22_TPRATE_WORLD,
        # C3S_CMCC4_T2M_WORLD,
        # C3S_CMCC4_TPRATE_WORLD,
        # C3S_NCEP2_T2M_WORLD,
        # C3S_NCEP2_TPRATE_WORLD,
        # C3S_JMA4_T2M_WORLD,
        # C3S_JMA4_TPRATE_WORLD,
        # C3S_ECCC5_T2M_WORLD,
        # C3S_ECCC5_TPRATE_WORLD,
        # C3S_BOM2_T2M_WORLD,
        # C3S_BOM2_TPRATE_WORLD,

        # C3S_JMA3_T2M_WORLD,
        # C3S_JMA3_TPRATE_WORLD,

        # C3S_UKMO604_T2M_WORLD,
        # C3S_UKMO604_TPRATE_WORLD,
    ]
    
    for cls in classes:
        try:
            print(f'Processing {cls.__name__}...')
            c3s = cls(download_from_origin=True, date_from='2026-01-01', verbose=True, assume_local_complete=True)
            c3s.update()
        except Exception as ex:
            raise

   
    # mr = MeteoRaster.load(c3s.data_index['stored_file'].iloc[-1])
    # mr.plot_mean(coastline=True, borders=True)

    # mr = None
    # for mr0 in c3s.data_index.loc[c3s.data_index['stored_file_complete'], 'stored_file'].unique():
    #     if mr is None:
    #         mr = MeteoRaster.load(mr0)
    #     else:
    #         mr.join(MeteoRaster.load(mr0))
    # mr.plot_mean(coastline=True, borders=True)

    # kml = Path(r'C:\Users\zepedro\Downloads\zones.kml')
    # agg, centroids = mr.get_values_from_KML(kml, nameField='zone')
    # mean = agg.stack(['zone', 'leadtime']).mean(axis=1).unstack(['zone', 'leadtime'])
    # agg.to_excel('C3S_ecmwf_full.xlsx')
    # mean.to_excel('C3S_ecmwf_mean.xlsx')
    # mr.get_values_from_latlon(-20, 16).stack('leadtime')
    pass