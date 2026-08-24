'''
ERA5M -- ERA5-Land monthly means, world domain.

Motivation
----------
ERA5 (era5.py) and ERA5W (era5w.py) acquire ERA5-Land *hourly* data. A world
month of hourly data is enormous, which is why ERA5W has to chunk it. The
monthly-mean product carries a single field per month instead of ~744, so the
full world grid at 0.1 degrees is cheap to acquire and is stored uncropped.

Design decisions
----------------
* World only: no KML, so `storage_bounding_box` stays None and BaseTask.store()
  skips its crop (base.py, "Reduce footpring for storage"). That is the whole
  mechanism -- `create_kml_classes` is deliberately not called.
* No `area` in the CDS request: the native 0..360 grid is delivered untouched and
  MeteoRaster.__init__ normalises it to -180..180 (_fixStartAtGreenwich).
* Local files are one bare grib per month (download_format='unarchived'), so none
  of ERA5's zip/unpack/parquet machinery applies.
* Stored files hold a whole year (12 monthly steps). MeteoRaster.save() compresses
  only the data variable, so the 2-D lat/lon meshgrids cost ~99 MiB per file at
  world resolution; grouping by year amortises that over 12 months.
* Monthly means of accumulated variables (tp) are mean *daily* accumulations, so
  converting to a monthly total needs a days_in_month factor -- unlike the hourly
  product, where a bare *1000 is right.
'''

from tethys_tasks import BaseTask, CaptureNewVariables, DownloadMonitor
import pandas as pd
import numpy as np
import xarray as xr
from pathlib import Path
from meteoraster import MeteoRaster
import cdsapi
import shutil
import tempfile
import random
import string
import os
import inspect
from concurrent.futures import ThreadPoolExecutor, as_completed

# MeteoRaster.load() only accepts dtype from v3.0 and the deployed wheel can be older
# (v2.2 at the time of writing). Asking for float32 halves the ~600 MB a yearly world
# cube (12x1801x3600) would otherwise be held at.
_LOAD_FLOAT32 = {"dtype": "float32"} if "dtype" in inspect.signature(MeteoRaster.load).parameters else {}


class ERA5M_T2M_WORLD(BaseTask):
    '''
    ERA5-Land monthly averaged reanalysis, full world grid.
    https://cds.climate.copernicus.eu/datasets/reanalysis-era5-land-monthly-means
    '''

    with CaptureNewVariables() as _ERA5M_T2M_WORLD_VARIABLES: #It is essential that the format of the variable here is _CLASSnAME_VARIABLES
        # DATE_FROM must be set: BaseTask's default (utcnow-7d) is floored to the
        # current month start while date_to is utcnow-PUBLICATION_LATENCY, which
        # leaves populate() with an empty range on most days of the month -- and
        # its `index['leadtime'][0]` is not guarded against that.
        DATE_FROM = (pd.Timestamp.utcnow() - pd.DateOffset(months=6)).strftime('%Y-%m-%d')

        # Month M is published around the 6th of M+1, i.e. ~37 days after M starts.
        PUBLICATION_LATENCY = pd.Timedelta(days=40)
        PRODUCTION_FREQUENCY = pd.DateOffset(months=1)
        # Worst case, the newest available month start is ~70 days old.
        FAIL_IF_OLDER = pd.Timedelta(days=80)

        # LEADTIMES and STORAGE_SEARCH_WINDOW are inherited on purpose: the base
        # defaults (a single zero leadtime, 14 months) are what a yearly stored
        # file needs. Copying era5/c3s's 40-day window would make store() build
        # year files containing only the months around the run date.

        SOURCE_PARALLEL_TRANSFERS = 2

        # CDS download log verbosity: 'silent' | 'info' | 'debug' (env-overridable).
        CDS_VERBOSITY = os.getenv('CDS_VERBOSITY', 'info').lower()
        # CDS download progress bar (ignored when CDS_VERBOSITY == 'silent').
        CDS_PROGRESS = os.getenv('CDS_PROGRESS', 'False').lower() in ('true', '1', 't')

        PIXEL_SIZE = 0.1

        VARIABLE = 't2m'
        ZONE = 'world'

        # False: the cloud tier writes blobs straight to their final path and does
        # not remove a partial file, so existence alone must not be trusted.
        # read_local_completeness() validates each grib once (cheaply) instead.
        ASSUME_LOCAL_COMPLETE = False

        CLOUD_TEMPLATE = 'ERA5M_{self._variable_upper}/era5m_{self._variable}_world/%Y/era5m_{self._variable}_%Y.%m.grib'
        LOCAL_PATH_TEMPLATE = 'ERA5M_{self._variable_upper}/era5m_{self._variable}_world/%Y/era5m_{self._variable}_%Y.%m.grib'
        STORAGE_PATH_TEMPLATE = 'ERA5M_{self._variable_upper}/era5m_{self._variable}_world/%Y/tethys_era5m_{self._variable}_%Y.nct'

        DATASET = 'reanalysis-era5-land-monthly-means'
        PRODUCT_TYPE = 'monthly_averaged_reanalysis'

        VARIABLE_DICT = dict(
            t2m = '2m_temperature',
            tp = 'total_precipitation',
            sd = 'snow_depth_water_equivalent',
        )

    # ------------------------------------------------------------------ helpers
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

    @staticmethod
    def _grib_looks_intact(grib_file) -> bool:
        '''
        Structural check with a plain file handle: 'GRIB' at the start and '7777' at
        the end. It must run BEFORE cfgrib, which on Windows keeps the file open after
        a failed open -- the file could then never be deleted nor overwritten by a
        re-download, wedging that month permanently.
        '''
        try:
            with open(grib_file, 'rb') as f:
                if f.read(4) != b'GRIB':
                    return False
                f.seek(-4, os.SEEK_END)
                return f.read(4) == b'7777'
        except OSError:
            return False

    @staticmethod
    def _grib_production_datetime(grib_file) -> pd.Timestamp:
        '''
        Validates a grib and returns its production datetime, reading coordinates
        only -- values are never materialised, so this stays cheap on a world grid.
        Raises OSError on anything unexpected.

        `step` is deliberately ignored: monthly-mean messages expose it as a 0-d
        zero or not at all depending on the cfgrib/eccodes build.
        '''
        if not ERA5M_T2M_WORLD._grib_looks_intact(grib_file):
            raise OSError('Not a complete grib (GRIB/7777 markers missing).')

        with xr.open_dataset(grib_file, engine='cfgrib', indexpath='') as ds:
            variable_list = list(ds.data_vars)
            if len(variable_list) != 1:
                raise OSError(f'Expected exactly one data variable, found {variable_list}.')
            # atleast_1d covers the 0-d coordinate cfgrib produces for a single field.
            production_datetime = np.atleast_1d(ds['time'].data)
            if production_datetime.size != 1:
                raise OSError(f'Expected a single monthly field, found {production_datetime.size} time steps.')

        return pd.Timestamp(production_datetime[0])

    @staticmethod
    def _no_valid_steps() -> pd.Series:
        '''Empty (production_datetime, leadtime) series, shaped for the index.'''
        index = pd.MultiIndex.from_arrays(
            [pd.DatetimeIndex([]), pd.to_timedelta([])],
            names=['production_datetime', 'leadtime'])
        return pd.Series([], index=index, dtype=bool)

    # --------------------------------------------------------------- downloads
    def _download_cds_month(self, variables):
        '''
        Downloads one month. `variables` is (request_options, local_path).
        The response is a bare grib (download_format='unarchived'); it is validated
        and only then moved into place, so a failed transfer leaves nothing behind.
        '''
        options, local_path = variables
        local_path_ = Path(local_path)
        expected = pd.Timestamp(year=int(options['year']), month=int(options['month']), day=1)

        c = self._cds_client()
        workdir = Path(tempfile.mkdtemp(prefix='era5m_dl_'))
        try:
            dl = workdir / 'download.grib'
            c.retrieve(self._dataset, options).download(str(dl))

            found = self._grib_production_datetime(dl)
            if found != expected:
                raise OSError(f'Grib holds {found.strftime("%Y-%m")}, expected {expected.strftime("%Y-%m")}.')

            local_path_.parent.mkdir(parents=True, exist_ok=True)
            if local_path_.exists():
                local_path_.unlink()
            shutil.move(str(dl), str(local_path_))

            return ((True, local_path))
        except Exception as ex:
            print(f'Download failed ({local_path_.name}): {ex}')
            return ((False, local_path))
        finally:
            shutil.rmtree(workdir, ignore_errors=True)

    def _download_from_source(self) -> bool:
        '''
        Downloads missing months directly from CDS.

        Returns True if downloads were made.
        '''

        self.diag('    Download from source...', 1)

        to_retrieve = self.data_index.loc[~self.data_index['data_exists'], :]
        files_to_download = to_retrieve['local_file'].unique()
        if len(files_to_download) == 0:
            self.diag('        Nothing to download.', 1)
            return False

        info = []
        for local_path in files_to_download:
            rows = self.data_index.loc[self.data_index['local_file'] == local_path]
            date = pd.Timestamp(rows['production_datetime'].iloc[0]).replace(day=1, hour=0)

            # Never request a month that is not published yet.
            if date > self.last_production_datetime:
                continue

            options = {'data_format': 'grib',
                       'download_format': 'unarchived',
                       'product_type': [self._product_type],
                       'variable': [self._variable_dict[self._variable]],
                       'year': f'{date.year}',
                       'month': f'{date.month:02d}',
                       'time': ['00:00'],
                       'nocache': ''.join(random.choice(string.digits) for _ in range(6)),
                       }
            info.append((options, local_path))

        if not info:
            self.diag('        Nothing to download.', 1)
            return False

        self.diag(f'        Downloading ({self._source_parallel_transfers} threads).', 1)
        downloaded = False
        with DownloadMonitor() as monitor:
            with ThreadPoolExecutor(max_workers=self._source_parallel_transfers) as executor:
                futures = [executor.submit(self._download_cds_month, i) for i in info[::-1]]
                for future in as_completed(futures):
                    state, local_path_ = future.result()
                    if state:
                        self.data_index.loc[self.data_index['local_file'] == local_path_, 'local_file_exists'] = True
                        downloaded = True
                        self.diag('        ' + monitor.mark_success(local_path_), 1)
                    else:
                        self.diag(f'        Download failed for {Path(local_path_).name}.', 1)

        return downloaded

    # ------------------------------------------------------------------- reads
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

            production_datetime = np.atleast_1d(ds.time.data)
            if production_datetime.size != 1:
                raise Exception(f'Expected a single monthly field, found {production_datetime.size} time steps.')
            data['production_datetime'] = production_datetime

            # cfgrib squeezes time/step to scalar coords, so a single monthly field
            # comes back 2-D. Reshape explicitly to the 5-D MeteoRaster layout
            # [production, ensemble_member, leadtime, lat, lon]: MeteoRaster does not
            # validate ndim and would otherwise store a structurally wrong cube.
            values = ds[variable].compute().values
            if values.ndim not in (2, 3):
                raise Exception(f'Unexpected field shape {values.shape}.')
            data['data'] = values.reshape((1, 1, 1) + values.shape[-2:])

        return data

    def _read_helper(self, grib_file:str) -> dict:
        '''
        Reads one grib file
        '''

        try:
            # No variable name is passed: the short name of a monthly-mean field is
            # not guaranteed to match our key, and the file holds only one variable.
            data = self._read_file(grib_file)
        except Exception as ex:
            raise Exception(f'{str(ex)} ({self.__class__.__name__}).')

        return data

    def read_local(self, local_file: str) -> MeteoRaster:
        '''
        Returns a MeteoRaster object with one month of ERA5-Land monthly means
        '''

        self.diag(f'            Reading "{local_file}" ({self.__class__.__name__})', 1)

        if not Path(local_file).exists():
            raise Exception('Local file does not exit.')

        data = self._read_helper(local_file)

        # store() places the read into a slot chosen by production_datetime, where a
        # mismatch surfaces as an opaque shape error. Check it against the index here.
        expected = self.data_index.loc[self.data_index['local_file']==local_file, 'production_datetime']
        found = pd.Timestamp(data['production_datetime'][0])
        if len(expected)>0:
            expected = pd.Timestamp(expected.iloc[0])
            if found != expected:
                raise Exception(f'"{Path(local_file).name}" holds {found.strftime("%Y-%m")}, expected {expected.strftime("%Y-%m")} ({self.__class__.__name__}).')
            data['production_datetime'] = np.array([expected.to_datetime64()])
            found = expected

        if self._variable == 'tp':
            # Monthly means of accumulations are mean DAILY accumulations (m/day).
            data['data'] = data['data'] * (1000 * found.days_in_month)
            units = 'mm/month'
        elif self._variable == 't2m':
            data['data'] = data['data'] - 273.15
            units = 'C'
        elif self._variable == 'sd':
            data['data'] = data['data'] * 1000
            units = 'mm'
        else:
            units = 'unknown'

        data['leadtimes'] = np.array([pd.Timedelta('0D')])

        # trim() is skipped on purpose (era5/c3s call it): a monthly file holds a
        # single production step, so there is nothing to trim, and it raises
        # IndexError on an all-NaN field.
        return MeteoRaster(data, units=units, variable=self._variable, verbose=False)

    def read_local_completeness(self, local_file:str) -> pd.Series:
        '''
        Returns the single valid (production_datetime, leadtime) step of a local file
        without decoding any values.

        A grib that cannot be read is discarded (and removed when the OS allows): the
        cloud tier writes blobs straight to their final path without cleaning up a
        partial transfer, and the base code calls read_local() unguarded, so a
        truncated file trusted on existence alone would abort every subsequent run.
        '''

        try:
            production_datetime = self._grib_production_datetime(local_file)
        except Exception as ex:
            print(f'        Local file unreadable, discarding it: {local_file} ({ex}).')
            # Best effort. _grib_looks_intact keeps cfgrib away from a truncated file so
            # this normally succeeds; a file that passes it but still fails to decode stays
            # locked by cfgrib on Windows. Returning no valid steps is what protects the
            # run either way: store() only reads files whose steps exist.
            try:
                Path(local_file).unlink(missing_ok=True)
            except OSError as unlink_ex:
                print(f'            Could not remove it yet ({unlink_ex}).')
            self.data_index.loc[self.data_index['local_file']==local_file, 'local_file_exists'] = False
            return self._no_valid_steps()

        index = pd.MultiIndex.from_arrays(
            [pd.DatetimeIndex([production_datetime]), pd.to_timedelta([pd.Timedelta('0D')])],
            names=['production_datetime', 'leadtime'])
        valid_steps = pd.Series(True, index=index)

        return valid_steps.loc[valid_steps.index.isin(self.data_index.index)]

    def _load_stored_file(self, stored_file:str):
        '''
        Same contract as BaseTask (None when unreadable), but asks for float32 where
        the installed MeteoRaster supports it (see _LOAD_FLOAT32).
        '''

        try:
            return MeteoRaster.load(stored_file, verbose=False, **_LOAD_FLOAT32)
        except Exception as ex:
            print(f'        Stored file unreadable, it will be rebuilt: {stored_file} ({ex}).')
            return None


class ERA5M_TP_WORLD(ERA5M_T2M_WORLD):
    with CaptureNewVariables() as _ERA5M_TP_WORLD_VARIABLES: #It is essential that the format of the variable here is _CLASSNAME_VARIABLES
        VARIABLE = 'tp'


class ERA5M_SD_WORLD(ERA5M_T2M_WORLD):
    with CaptureNewVariables() as _ERA5M_SD_WORLD_VARIABLES: #It is essential that the format of the variable here is _CLASSNAME_VARIABLES
        VARIABLE = 'sd'


if __name__=='__main__':
    import matplotlib.pyplot as plt
    plt.ion()

    kwargs = dict(download_from_origin=True,
                  date_from='2026-03-01')

    task = ERA5M_T2M_WORLD(**kwargs)
    # task = ERA5M_TP_WORLD(**kwargs)
    # task = ERA5M_SD_WORLD(**kwargs)
    task.update()

    # mr = MeteoRaster.load(task.data_index['stored_file'].unique()[-1])
    # mr.plot_mean(coastline=True, borders=True)
    # mr.get_values_from_latlon(47, 8)

    # docker-compose run --rm tethys-tasks ERA5M_T2M_WORLD update --class_kwargs "{\"download_from_origin\": \"True\", \"date_from\": \"'2025-06-01'\"}"

    pass
