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

class C3S_ECMWF(BaseTask):
    '''
    Docstring for C3S_ECMWF
    '''

    with CaptureNewVariables() as _C3S_ECMWF_VARIABLES: #It is essential that the format of the variable here is _CLASSnAME_VARIABLES
        PUBLICATION_LATENCY = pd.Timedelta(days=8)
        PRODUCTION_FREQUENCY = pd.DateOffset(months=1)
        FAIL_IF_OLDER = pd.Timedelta(days=35)
                
        LEADTIME_MONTH = ['1', '2', '3', '4', '5', '6']
        LEADTIMES = [pd.DateOffset(months=int(m)) for m in LEADTIME_MONTH]

        SOURCE_PARALLEL_TRANSFERS = 3

        C3S_SYSTEM = '51'
        ORIGINATING_CENTRE = 'ecmwf'
        PIXEL_SIZE = 0.1


        ASSUME_LOCAL_COMPLETE = True


        CLOUD_TEMPLATE = 'test/C3S_ECMWF_{self._variable_upper}/c3s_ecmwf_{self._variable}_{self._zone}/%Y/c3s_ecmwf_{self._variable}_%Y.%m.grib'
        LOCAL_PATH_TEMPLATE = 'C3S_ECMWF_{self._variable_upper}/c3s_ecmwf_{self._variable}_{self._zone}/%Y/c3s_ecmwf_{self._variable}_%Y.%m.grib'
        STORAGE_PATH_TEMPLATE = 'C3S_ECMWF_{self._variable_upper}/c3s_ecmwf_{self._variable}_{self._zone}/%Y/tethys_c3s_ecmwf_{self._variable}_%Y.%m.nct'

        STORAGE_SEARCH_WINDOW = pd.DateOffset(days=40)

        VARIABLE_DICT = dict(
            t2m = '2m_temperature',
            tprate = 'total_precipitation',
        )

        CUMULATIVE = dict(
            tp=True,
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
            days = ['%02d' % d for d in range(1, calendar.monthrange(date.year, date.month)[1]+1)]
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
            data['data'] = np.expand_dims(ds[variable][...].data, axis=0)
            data['leadtimes'] = [pd.DateOffset(months=int(m)) for m in np.round(ds.step.data.astype(float)/1E9/86400/30, 0)]

            seconds = np.diff(ds.step.data/1E9, prepend=0).astype(float)

        if variable=='tprate':
            for i0, s0 in enumerate(seconds):
                data['data'][:, :, i0, ...] *= seconds[i0] * 1000 # mm/month

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
        Returns a MeteoRaster object with the ERA5 Land data
        '''
        
        self.diag(f'            Reading "{local_file}" ({self.__class__.__name__})', 1)

        data = self._read_helper(local_file)
                
        if self._variable == 'tprate':
            units = 'mm/month'
        elif self._variable == 't2m':
            data['data'] -= 273.15
            units = 'C'
                
        tmp = MeteoRaster(data, units=units, variable=self._variable, verbose=False)
        tmp.trim()
        # tmp.getDataFromLatLon(26, -14).to_clipboard(excel=True)
        
        return tmp

# creates regional classes such as C3S_ECMWF_TPRATE_ZAMBEZI, etc...
variable_kwargs = {'VARIABLE': ['t2m', 'tprate']}
create_kml_classes(C3S_ECMWF, variable_kwargs)

class C3S_ECMWF_T2M_WORLD(C3S_ECMWF):
    with CaptureNewVariables() as _C3S_ECMWF_T2M_WORLD_VARIABLES: #It is essential that the format of the variable here is _CLASSNAME_VARIABLES
        VARIABLE='t2m'
        ZONE='world'

class C3S_ECMWF_TPRATE_WORLD(C3S_ECMWF):
    with CaptureNewVariables() as _C3S_ECMWF_TPRATE_WORLD_VARIABLES: #It is essential that the format of the variable here is _CLASSNAME_VARIABLES
        VARIABLE='tprate'
        ZONE='world'

if __name__=='__main__':
    import matplotlib.pyplot as plt
    plt.ion()

    c3s = C3S_ECMWF_TPRATE_CAUCASUS(download_from_source=True, date_from='2026-01-01')
    # era5 = ERA5_BELGIUM_TP(download_from_source=True, date_from='2021-01-01', source_parallel_transfers=2)
    c3s.retrieve_store_upload_and_cleanup()
    # era5.retrieve()
    # era5.upload_to_cloud()

    # mr = MeteoRaster.load(r'C:\tethys-tasks storage test\ERA5_T2M\era5_t2m_belgium\2026\tethys_era5_t2m_2026.01.01.nct')
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