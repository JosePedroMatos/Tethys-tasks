from tasks import BaseTask, CaptureNewVariables, DownloadMonitor
import pandas as pd
import numpy as np
import xarray as xr
from pathlib import Path
import shutil
import tempfile
import os
import urllib.request
from meteoraster import MeteoRaster
import numpy as np
from zipfile import ZipFile
from concurrent.futures import ThreadPoolExecutor, as_completed
from uuid import uuid4

class GFS_025_T2M_CAUCASUS(BaseTask):
    '''
    Docstring for GFS 0.25 air temperature data for the caucasus region
    '''

    with CaptureNewVariables() as _GFS_025_T2M_CAUCASUS_VARIABLES: #It is essential that the format of the variable here is _CLASSnAME_VARIABLES
        PUBLICATION_LATENCY = pd.Timedelta(hours=8)
        PUBLICATION_MEMORY = pd.Timedelta(days=10)
        PRODUCTION_FREQUENCY = pd.Timedelta(hours=24)
        LEADTIMES = pd.timedelta_range('0h', '384h', freq='3h')

        SOURCE_PARALLEL_TRANSFERS = 1

        CLOUD_TEMPLATE = 'test/NOAA_GFS_0.25/%Y/%m/%H/{{leadtime_hours}}/gfs_4_%Y.%m.%d_%H_{{leadtime_hours}}.nc'
        LOCAL_PATH_TEMPLATE = 'NOAA_GFS_0.25/%Y/%m/%H/{{leadtime_hours}}/gfs_4_%Y.%m.%d_%H_{{leadtime_hours}}.nc'
        STORAGE_PATH_TEMPLATE = 'NOAA_GFS_0.25/gfs_{self._variable_lower}_{self._zone}/%Y/tethys_NOAA_GFS_0.25_{{floor_7_days}}.nct'

        STORAGE_SEARCH_WINDOW = pd.DateOffset(days=10)

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

        ZONE = 'caucasus'
        STORAGE_KML='resources/gfs caucasus.kml'
        VARIABLE = 'TMP'
        VARIABLE_LOWER = 'tmp'

    def _7_days(self, production_datetime):
        reference = pd.Timestamp('1900-01-01')
        step = pd.Timedelta(days=7)
        return (reference + ((production_datetime - reference) // step) * step).dt.strftime('%Y.%m.%d')

    def _leadtime_hours(self, leadtime):
        return (leadtime // pd.Timedelta(hours=1)).astype(str).str.zfill(3)

    def populate(self, *args, **kwargs):
        # Add each 7 days (day_f7)
        additional_columns = {'floor_7_days': lambda x: self._7_days(x['production_datetime']),
                              'leadtime_hours': lambda x: self._leadtime_hours(x['leadtime']),
                              }

        return super().populate(additional_columns=additional_columns, *args, **kwargs)

    def __download_helper(self, url: str, destination: str) -> tuple[bool, str]:
        """Download url into a system temp file and move to destination on success."""

        dest_path = Path(destination)
        dest_path.parent.mkdir(parents=True, exist_ok=True)

        fd = None
        temp_file = None

        try:
            request = urllib.request.Request(
                url,
                headers={
                    'User-Agent': 'Mozilla/5.0',
                    'Accept': '*/*',
                    'Accept-Encoding': 'identity',
                },
            )
            with urllib.request.urlopen(request) as response:
                status = response.getcode()
                if status is not None and not (200 <= status < 300):
                    raise Exception(f'HTTP status {status}')

                fd, temp_path = tempfile.mkstemp(prefix=f'{dest_path.stem}.', suffix=f'.{uuid4().hex}.part')
                temp_file = Path(temp_path)

                with os.fdopen(fd, 'wb') as handle:
                    shutil.copyfileobj(response, handle)

            temp_file.replace(dest_path)
            return True, str(dest_path)
        except Exception as ex:
            if fd is not None:
                try:
                    os.close(fd)
                except OSError:
                    pass
            if temp_file is not None and temp_file.exists():
                temp_file.unlink()
            print(f'        Error downloading {url} -> {dest_path}: {ex}.')
            return False, str(dest_path)

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
        to_download['url'] = to_download['production_datetime'].dt.strftime(url_template)
        if '{' in url_template:
            to_download['url'] = to_download.apply(lambda x: x['url'].format(**x.to_dict()), axis=1)

        self.diag(f'        Downloading ({self._source_parallel_transfers} threads).', 1)
        downloaded = False
        with DownloadMonitor() as monitor:
            with ThreadPoolExecutor(max_workers=self._source_parallel_transfers) as executor:
                futures = {executor.submit(self.__download_helper, r['url'], r['local_file']): (r['local_file'], r['url']) for _, r in to_download.iterrows()}
                for future in as_completed(futures):
                    success, downloaded_file = future.result()
                    if success:
                        msg = monitor.mark_success(downloaded_file)
                        self.diag('        ' + msg, 1)

        if downloaded:
            self._check_existing_data(stored=False, cloud=False)

        return downloaded

    def read_local(self, local_file: str) -> MeteoRaster:
        '''
        Returns a MeteoRaster object with the data
        '''
        
        self.diag(f'            Reading "{local_file}" ({self.__class__.__name__})', 1)


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
                raise Exception('NetCDF support to be checked')
                latitudes = ds.lat.data
                longitudes = ds.lon.data
                production_datetimes = np.array(pd.to_datetime(ds.productionDatetimes.data))
                leadtimes = pd.arrays.TimedeltaArray._from_sequence([pd.Timedelta(hours=ds.leadtimes.data[0]/1E9/3600)])
                data_ = ds[self._variable][...].data
        elif file_type=='grib':
            with xr.open_dataset(local_file, engine='cfgrib', **self._backend_kwargs[self._variable]) as ds:
                latitudes = ds.latitude.data
                longitudes = ds.longitude.data
                production_datetime = pd.to_datetime(np.expand_dims(ds.time.data, 0))
                leadtimes = pd.to_timedelta(pd.to_datetime(np.expand_dims(ds.valid_time.data, 0))-production_datetime[0])
                
                data_ = ds[self._grib_variable[self._variable]][...].data
            
                if self._variable=='TMP':
                    data_ -= 273.15

        data = np.full([1, 1, self._leadtimes.shape[0]] + list(data_.shape), np.NaN)
        lt_idx = np.searchsorted(self._leadtimes, leadtimes[0])
        data[0, 0, lt_idx, ...] = data_
        leadtimes = self._leadtimes

        data=dict(data=data,
                  latitudes=latitudes,
                  longitudes=longitudes,
                  production_datetime=production_datetime,
                  leadtimes=leadtimes,
                  )

        units = self._units[self._variable]

        mr = MeteoRaster(data=data, units=units, variable=self._variable, verbose=False)

        return mr

class GFS_025_PCP_CAUCASUS(GFS_025_T2M_CAUCASUS):
    '''
    Docstring for GFS 0.25 air temperature data for the caucasus region
    '''

    with CaptureNewVariables() as _GFS_025_PCP_CAUCASUS_VARIABLES: #It is essential that the format of the variable here is _CLASSnAME_VARIABLES
        VARIABLE = 'PRATE'
        VARIABLE_LOWER = 'pcp'


if __name__=='__main__':
    import matplotlib.pyplot as plt
    plt.ion()

    alaro = GFS_025_PCP_CAUCASUS(download_from_source=False, date_from='2026-01-29')    
    alaro.retrieve_and_upload()
    # alaro.retrieve()
    # alaro.upload_to_cloud()
    alaro.store()

    pass