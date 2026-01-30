from tasks import BaseTask, CaptureNewVariables, DownloadMonitor
import pandas as pd
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

class ALARO40L_T2M(BaseTask):
    '''
    Docstring for ALARO 40 L air temperature data
    '''

    with CaptureNewVariables() as _ALARO40L_T2M_VARIABLES: #It is essential that the format of the variable here is _CLASSnAME_VARIABLES
        PUBLICATION_LATENCY = pd.Timedelta(hours=5)
        PUBLICATION_MEMORY = pd.Timedelta(hours=26)
        PRODUCTION_FREQUENCY = pd.Timedelta(hours=6)
        LEADTIMES = pd.timedelta_range('0h', '60h', freq='1h')

        SOURCE_PARALLEL_TRANSFERS = 1 # It is extremely fast. No need for parallel.

        CLOUD_TEMPLATE = 'test/ALARO_IRM/%Y/%m/alaro40l_%Y%m%d%H.zip'
        LOCAL_PATH_TEMPLATE = 'ALARO_IRM/%Y/%m/alaro40l_%Y%m%d%H.zip'
        STORAGE_PATH_TEMPLATE = 'ALARO_IRM/alaro40l_{self._variable}/%Y/%m/tethys_alaro40l_{self._variable}_%Y.%m.%d.nct'
        # STORAGE_PATH_TEMPLATE = 'ERA5_{self._variable_upper}/era5_{self._variable}_{self._zone}/%G/tethys_era5_{self._variable}_%G.%V.nc'

        DOWNLOAD_TEMPLATE = 'https://opendata.meteo.be/ftp/forecasts/alaro_40l/%Y%m%d%H/{file}'

        # https://opendata.meteo.be/resources/forecasts/alaro/params227-228.tab
        VARIABLE_DICT = dict(t2m='2T', # 2 m temperature [K or C]
                             cr='CR', # Convective rain [m]
                             cs='CS', # Convective snow [m]
                             lss ='LSS', # Large scale snow [m]
                             tp='TotPrecip', # Total precipitation [m]

                            #  swe='SM', # Snow mass [kg m**-2]
                            #  csr='CSR', # Clim Snow Reservoir [m (water equiv)]
                             )
        VARIABLE_LIST = [i for k, i in VARIABLE_DICT.items()]

        VARIABLE = 't2m'

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

        data_to_retrieve_from_source = self.data_index.loc[~self.data_index['data_exists'], ['production_datetime', 'local_file']].groupby('production_datetime').first().iloc[:, 0]

        now_in_belgium = ts_be = pd.Timestamp.now(tz="Europe/Brussels").tz_localize(None)
        data_to_retrieve_from_source = data_to_retrieve_from_source.loc[data_to_retrieve_from_source.index>=now_in_belgium - self._publication_memory - self._publication_latency]

        if data_to_retrieve_from_source.shape[0]==0:
            self.diag('        Nothing to download.', 1)
            return False

        with tempfile.TemporaryDirectory() as tmp_dir:

            download_rows = []
            download_groups = {}
            for dt0, f0 in data_to_retrieve_from_source.items():
                tmp = []
                for v1 in self._variable_list:
                    filename = dt0.strftime(f'alaro40l_%Y%m%d%H_{v1}.grb')
                    url = dt0.strftime(self._download_template.format(file=filename))
                    local_path = Path(tmp_dir) / filename

                    tmp.append(dict(production_datetime=dt0, local_file=f0, download_file=str(local_path), url=url))
                
                download_rows += tmp
                download_groups[f0] = tmp

            to_download = pd.DataFrame(download_rows)

            self.diag(f'        Downloading ({self._source_parallel_transfers} threads).', 1)
            downloaded = False

            with DownloadMonitor() as monitor:
                with ThreadPoolExecutor(max_workers=self._source_parallel_transfers) as executor:
                    futures = {executor.submit(self.__download_helper, r['url'], r['download_file']): (r['local_file'], r['url']) for _, r in to_download.iterrows()}
                    for future in as_completed(futures):
                        success, downloaded_file = future.result()
                        if success:
                            msg = monitor.mark_success(downloaded_file)
                            self.diag('        ' + msg, 1)

                            local_file, url = futures[future]

                            complete = True
                            for f1 in download_groups[local_file]:
                                if not Path(f1['download_file']).exists():
                                    complete = False
                                    continue

                            try:
                                if complete:
                                    local_file_ = Path(local_file)
                                    local_file_.parent.mkdir(parents=True, exist_ok=True)

                                    fd_zip, tmp_zip_path = tempfile.mkstemp(prefix=f'{local_file_.stem}.', suffix='.part', dir=tmp_dir)
                                    with ZipFile(tmp_zip_path, 'w') as zipf:
                                        for f1 in download_groups[local_file]:
                                            fpath = Path(f1['download_file'])
                                            zipf.write(fpath, arcname=fpath.name)
                                            fpath.unlink()
                                    os.close(fd_zip)
                                    tmp_zip = Path(tmp_zip_path)
                                    tmp_zip.replace(local_file_)

                                    self.diag(f'        Saved {local_file_.name}.', 1)

                            except Exception as ex:
                                self.diag(f'        Error creating {local_file_.absolute()}.', 1)
                        else:
                            self.diag(f'        Download failed ().', 1)

        if downloaded:
            self._check_existing_data(stored=False, cloud=False)

        return downloaded

    def _read_local_helper(self, variable_file: str) -> dict:
        data = {}
        with xr.open_dataset(variable_file, engine='cfgrib', indexpath='') as ds:
            data['latitudes'] = ds.latitude.data[:-1]
            data['longitudes'] = ds.longitude.data
            data['production_datetime'] = pd.to_datetime(np.array((ds.time.data,)))
            data['leadtimes'] = pd.to_timedelta(ds.valid_time.data-ds.time.data)
            data['data'] = np.expand_dims(ds['unknown'].data[:, :-1, :], axis=(0, 1))
    
    def read_local(self, local_file: str) -> MeteoRaster:
        '''
        Returns a MeteoRaster object with the data
        '''
        
        self.diag(f'            Reading "{local_file}" ({self.__class__.__name__})', 1)

        local_file = Path(local_file)

        with tempfile.TemporaryDirectory() as tmp_dir:
            with ZipFile(local_file, 'r') as zip_ref:
                file_list = zip_ref.namelist()
                target_file = [f for f in file_list if f'_{self._variable_dict[self._variable]}.grb' in f][0]
                zip_ref.extract(target_file, tmp_dir)
                extracted_file = Path(tmp_dir) / target_file

            data = self._read_local_helper(extracted_file)
        
        if self._variable == 't2m':
            data['data'] -= 273.15
            units = 'C'
        # elif self._variable == 'tp':
        #     data['data'] -= 273.15
        #     units = 'C'
        else:
            raise(f'Datatype {self._variable} not supported ({self.__class__.__name__})')
        
        mr = MeteoRaster(data, units=units, variable=self._variable, verbose=False)
        
        # mr.plot_mean(coastline=True, borders=True)
        
        return mr

class ALARO40L_TP(ALARO40L_T2M):
    '''
    Docstring for ALARO 40 L total precipitation data
    '''
    with CaptureNewVariables() as _ALARO40L_TP_VARIABLES: #It is essential that the format of the variable here is _CLASSnAME_VARIABLES
        VARIABLE = 'tp'

class ALARO40L_CR(ALARO40L_T2M):
    '''
    Docstring for ALARO 40 L convective rain data
    '''
    with CaptureNewVariables() as _ALARO40L_CR_VARIABLES: #It is essential that the format of the variable here is _CLASSnAME_VARIABLES
        VARIABLE = 'cr'

if __name__=='__main__':
    import matplotlib.pyplot as plt
    plt.ion()

    alaro = ALARO40L_TP(download_from_source=True, date_from='2026-01-28')
    alaro.read_local('tests/data/ALARO/2026012900.zip')
    
    # alaro.retrieve_and_upload()
    # alaro.retrieve()
    # alaro.upload_to_cloud()
    # alaro.store()

    pass