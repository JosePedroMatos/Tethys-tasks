from tethys_tasks import BaseTask, CaptureNewVariables, DownloadMonitor
import pandas as pd
import os
from pathlib import Path
import hashlib

_LOCAL_ECCODES_DEFINITIONS = (
    Path(__file__).resolve().parent / 'resources' / 'icon_ch' / 'eccodes_definitions'
)
if _LOCAL_ECCODES_DEFINITIONS.exists():
    os.environ.setdefault('ECCODES_DEFINITION_PATH', str(_LOCAL_ECCODES_DEFINITIONS))

from meteodatalab import data_source, grib_decoder, ogd_api
from earthkit.data import config
config.set('cache-policy', 'temporary')

import numpy as np
import xarray as xr
import shutil
import tempfile
import urllib.request
import urllib.error
from urllib.parse import urlparse
import threading
import time
from meteoraster import MeteoRaster
import numpy as np
from zipfile import ZIP_LZMA, ZipFile
from concurrent.futures import ThreadPoolExecutor, as_completed, ProcessPoolExecutor
from uuid import uuid4
from typing import Tuple

_GRIB_EXTENSIONS = {'.grib', '.grib2', '.grb'}

def unzip_and_load_gribs_as_xarray(
    zip_path: str | Path,
    variable: str,
) -> xr.DataArray:
    """Extract a GRIB zip archive and decode the requested variable to xarray."""
    zip_path = Path(zip_path)
    if not zip_path.exists():
        raise FileNotFoundError(zip_path)

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)
        with ZipFile(zip_path, mode='r') as zf:
            zf.extractall(tmp_path)

        grib_files = sorted(
            p for p in tmp_path.rglob('*')
            if p.is_file() and p.suffix.lower() in _GRIB_EXTENSIONS
        )
        if not grib_files:
            raise RuntimeError(f'No GRIB files found in archive {zip_path}.')

        horizontal_files = [
            p for p in grib_files if p.name.startswith('horizontal_constants_')
        ]
        if not horizontal_files:
            raise RuntimeError('No horizontal constants file found in archive.')

        coord_source = data_source.FileDataSource(
            datafiles=[str(p) for p in horizontal_files]
        )
        coord_fields = grib_decoder.load(
            coord_source,
            {'param': ['CLON', 'CLAT']},
            geo_coords=lambda _: {},
        )
        geo_coords = {
            'lon': coord_fields['CLON'].squeeze(),
            'lat': coord_fields['CLAT'].squeeze(),
        }

        source = data_source.FileDataSource(datafiles=[str(p) for p in grib_files])
        decoded = grib_decoder.load(
            source,
            {'param': variable},
            geo_coords=lambda _: geo_coords,
        )

        if variable not in decoded:
            available = ', '.join(sorted(decoded.keys()))
            raise KeyError(
                f'Variable {variable} not found in archive. Available variables: {available}'
            )

        return decoded[variable]

class ICON_CH2_EPS_TOT_PREC(BaseTask):
    '''
    Docstring for GFS ICON_CH2_EPS

    https://colab.research.google.com/github/MeteoSwiss/opendata-nwp-demos/blob/main/01_retrieve_process_precip.ipynb#scrollTo=TvqLrOwV0OBm
    '''

    with CaptureNewVariables() as _ICON_CH2_EPS_TOT_PREC_VARIABLES: #It is essential that the format of the variable here is _CLASSnAME_VARIABLES
        PUBLICATION_LATENCY = pd.Timedelta(hours=3)
        PUBLICATION_MEMORY = pd.Timedelta(hours=24)
        PRODUCTION_FREQUENCY = pd.Timedelta(hours=6)
        LEADTIMES = pd.timedelta_range('0h', '120h', freq='1h')

        CLOUD_TEMPLATE = 'forecasts/ICON_CH2_{self._variable}/%Y/%m/%d/icon_ch2_%Y.%m.%d_%H.zip'
        LOCAL_PATH_TEMPLATE = 'ICON_CH2{self._variable}/%Y/%m/%d/icon_ch2_%Y.%m.%d_%H.zip'
        STORAGE_PATH_TEMPLATE = 'ICON_CH2/icon_ch2_{self._variable_lower}/%Y/tethys_icon_ch2_{{floor_7_days}}.nct'

        STORAGE_SEARCH_WINDOW = pd.DateOffset(days=10)
        ASSUME_LOCAL_COMPLETE = False

        PIXEL_SIZE = 0.25

        PERMANENT_FILES = [Path(__file__).resolve().parent / 'resources' / 'icon_ch' / f for f in ['horizontal_constants_icon-ch2-eps.grib2',
                                                                                                   'horizontal_constants_icon-ch2-eps.sha256',
                                                                                                   'vertical_constants_icon-ch2-eps.grib2',
                                                                                                   'vertical_constants_icon-ch2-eps.sha256',
                                                                                                   ]]

        UNITS = dict(T_2M='C',         # original in K
                     TOT_PREC='mm/h', # original in mm (cumulative)
                     W_SNOW='mm',
                     U_10M='m/s',
                     V_10M='m/s',

                     )

        COLLECTION = 'ogd-forecasting-icon-ch2'
        VARIABLE = 'TOT_PREC'
        VARIABLE_LOWER = VARIABLE.lower()

    def _7_days(self, production_datetime):
        reference = pd.Timestamp('1900-01-01')
        step = pd.Timedelta(days=7)
        return (reference + ((production_datetime - reference) // step) * step).dt.strftime('%Y.%m.%d')

    def populate(self, *args, **kwargs):
        # Add each 7 days (floor_7_days)
        additional_columns = {'floor_7_days': lambda x: self._7_days(x['production_datetime']),
                              }

        return super().populate(additional_columns=additional_columns, *args, **kwargs)

    @staticmethod
    def __to_ogd_api_leadtimes(leadtimes):
        '''
        Returns a list of pd.Timedelta in the 'P0DT0H', 'P0DT1H', 'P0DT2H' format
        '''

        lts = []
        for lt0 in leadtimes:
            lts.append(f'P{lt0.days}DT{lt0.seconds//3600}H')

        return lts

    def __download_helper(self, leadtimes, ref_time, local_file):
        '''
        
        '''
        
        req = ogd_api.Request(
            collection=self._collection,
            variable=self._variable,
            ref_time=ref_time.isoformat() + 'Z',
            perturbed=False,
            lead_time=leadtimes,
        )

        with tempfile.TemporaryDirectory(prefix='icon_ch_payload_') as tmp_dir:
            out_dir = Path(tmp_dir)

            asset_urls = ogd_api.get_asset_urls(req)
            collection_id = f"ch.meteoschweiz.{req.collection}"
            model_suffix = req.collection.removeprefix('ogd-forecasting-')

            for f0 in self._permanent_files:
                shutil.copyfile(f0, out_dir / f0.name)

            coord_urls = [
                ogd_api.get_collection_asset_url(
                    collection_id,
                    f'{prefix}_constants_{model_suffix}-eps.grib2',
                )
                for prefix in ('horizontal', 'vertical')
            ]

            expected_files = {
                Path(urlparse(url).path).name
                for url in [*asset_urls, *coord_urls]
                if Path(urlparse(url).path).name
            }
            expected_total = len(expected_files)

            download_error = {'error': None}

            def _download_raw_payload():
                try:
                    ogd_api.download_from_ogd(req, out_dir)
                except Exception as ex:
                    download_error['error'] = ex

            download_thread = threading.Thread(target=_download_raw_payload, daemon=True)
            download_thread.start()

            seen_gribs = set()
            with DownloadMonitor() as monitor:
                while True:
                    grib_files = {
                        p.name
                        for p in out_dir.rglob('*')
                        if p.is_file() and p.suffix.lower() in _GRIB_EXTENSIONS
                    }
                    new_gribs = sorted(grib_files - seen_gribs)
                    for grib_name in new_gribs:
                        if grib_name not in [f.name for f in self._permanent_files]:
                            msg = monitor.mark_success(out_dir / grib_name)
                            self.diag('        ' + msg, 1)

                    seen_gribs.update(new_gribs)

                    if not download_thread.is_alive() or len(seen_gribs)==expected_total:
                        break
                    time.sleep(0.5)

            download_thread.join()
            if download_error['error'] is not None:
                raise download_error['error']

            # Create a zip (at local_file) with all the .grib2 files inside out_dir whose names are not in self._permanent_files
            permanent_names = {f.name for f in self._permanent_files}
            files_to_zip = sorted(
                p
                for p in out_dir.glob('*.grib2')
                if p.is_file() and p.name not in permanent_names
            )
            if not files_to_zip:
                raise RuntimeError('No non-permanent .grib2 files found to zip.')

            local_file_path = Path(local_file)
            local_file_path.parent.mkdir(parents=True, exist_ok=True)
            with ZipFile(local_file_path, mode='w', compression=ZIP_LZMA) as zf:
                for grib_file in files_to_zip:
                    zf.write(grib_file, arcname=grib_file.name)

    def _download_from_source(self) -> bool:
        '''
        Downloads missing files directly from the source

        Returns True of downloads were made
        '''

        self.diag('    Download from source...', 1)

        # Define leadtimes
        leadtimes = self.__to_ogd_api_leadtimes(self._leadtimes)

        # Check if there is a need to download
        to_download = self.data_index.loc[~self.data_index['data_exists'], :]
        if to_download.shape[0]==0:
            self.diag('        Nothing to download.', 1)
            return False

        # Define what files to download (each download should be complete)
        to_download_files = self.data_index.groupby('local_file').first()
        to_download_files = to_download_files.loc[to_download_files.production_datetime>=pd.Timestamp.utcnow().tz_localize(None) - self._publication_memory]

        # Download
        for download_file, associated_data in to_download_files.iterrows():
            try:
                self.__download_helper(leadtimes, associated_data.production_datetime, download_file)
                downloaded = True
            except Exception as ex:
                self.diag(f'        Problem downloading {Path(download_file).name} ({self.__class__.__name__}).', 1)

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

class ICON_CH2_EPS_T2M(ICON_CH2_EPS_TOT_PREC):
    
    with CaptureNewVariables() as _ICON_CH2_EPS_T2M_VARIABLES: #It is essential that the format of the variable here is _CLASSnAME_VARIABLES
        CLOUD_TEMPLATE = 'test/ICON_CH2/%Y/%m/%d/icon_ch2_%Y.%m.%d_%H.zip'
        LOCAL_PATH_TEMPLATE = 'ICON_CH2/%Y/%m/%d/icon_ch2_%Y.%m.%d_%H.zip'
        STORAGE_PATH_TEMPLATE = 'ICON_CH2/icon_ch2_{self._variable_lower}/%Y/tethys_icon_ch2_{{floor_7_days}}.nct'

        VARIABLE = 'T_2M'
        VARIABLE_LOWER = VARIABLE.lower()

if __name__=='__main__':
    import matplotlib.pyplot as plt
    plt.ion()

    task = ICON_CH2_EPS_T2M(download_from_source=True, date_from='2026-03-12 12:00:00')
    # task._update_index_and_completeness()

    # task = GFS_025_PCP_CAUCASUS(download_from_source=False, date_from='2025-01-01')

    task.retrieve_store_upload_and_cleanup()

    # # files = task.data_index['stored_file'].unique()
    # files = task.data_index.loc[task.data_index['stored_file_exists'], 'stored_file'].unique()
    # mr = None
    # for mr0 in files:
    #     if mr is None:
    #         mr = MeteoRaster.load(mr0)
    #     else:
    #         mr.join(MeteoRaster.load(mr0))
    # # mr.plot_mean(coastline=True, borders=True)
    # mr.get_values_from_latlon_by_event(mr.get_values_from_latlon(42.5,42.5)).bfill(axis=1).iloc[:, 0].plot()

    # task = GFS_025_PCP_BELGIUM(download_from_source=False, date_from='2026-01-01')    
    # task.retrieve_and_upload()
    # task.retrieve()
    # task.upload_to_cloud()

    pass