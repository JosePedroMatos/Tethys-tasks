'''
CERRA -- Copernicus European Regional ReAnalysis (reanalysis-cerra-single-levels).

Best-guess hourly fields (tp, t2m, snow-water equivalent) over Europe, stored on the
native 5.5 km Lambert-conformal grid (no regridding), cropped to a region.

Design decisions
----------------
* Native grid. CERRA GRIB is on a Lambert-conformal 5.5 km grid, so cfgrib exposes
  *2-D* ``latitude(y,x)`` / ``longitude(y,x)`` coordinate arrays already in WGS84
  degrees (eccodes computes them from the projection header -- no pyproj needed).
  MeteoRaster stores 2-D grids natively, so we keep the native cell layout and only
  need a 2-D-aware crop on read. Longitudes come in the 0..360 convention and are
  remapped to -180..180 to match the WGS84 KML bounding boxes.
* Hourly from forecast fields -- one uniform model for all three variables. CERRA runs
  a forecast from every 3-hourly reference time (00,03,...,21 UTC); leadtimes 1/2/3 h
  arrive in the SAME request. ``tp`` is accumulated from forecast start, so it is
  de-accumulated with a single ``np.diff`` along the step axis WITHIN one reference time
  (no cross-file boundary machinery, unlike ERA5); ``t2m`` and ``sd`` are instantaneous.
  read_local then collapses (reference + leadtime) into an hourly analysis-like series:
  ``production_datetime`` = event time, ``leadtime`` = 0.
* Reference vs event month. Hour 00:00 of a month is produced by the previous month's
  21:00 forecast, so the local/cloud (grib) files are keyed by the forecast REFERENCE
  month (matching the download) while the stored .nct is keyed by the EVENT (calendar)
  month. This gives gap-free natural calendar-month files without any stored boundary state.
* No server-side subsetting. CERRA has no ``area`` option, so the full European domain
  is downloaded per month and cropped locally (in ``_read_file``, before compute()).
* Not operational. ``PUBLICATION_LATENCY`` is nominal; drive coverage with ``date_to``.

Inherits the session unpack cache, zip handling, step-index cache and storage from ERA5.
'''

from tethys_tasks import BaseTask, CaptureNewVariables, create_kml_classes, DownloadMonitor
from tethys_tasks.era5 import ERA5
import pandas as pd
import numpy as np
import xarray as xr
from pathlib import Path
from meteoraster import MeteoRaster
import tempfile
import shutil
import random
import string
from zipfile import ZipFile, BadZipFile, ZIP_DEFLATED
from concurrent.futures import ThreadPoolExecutor, as_completed


class CERRA(ERA5):
    '''
    CERRA single-levels best-guess hourly fields on the native Lambert 5.5 km grid.
    Inherits reading cache and storage from ERA5; overrides the grid read, the (simple,
    boundary-free) processing and the CDS download for the CERRA request shape.
    '''

    with CaptureNewVariables() as _CERRA_VARIABLES:  # name MUST be _<ClassName>_VARIABLES
        # Nominal latency -- this driver is not operational, coverage is driven by date_to.
        PUBLICATION_LATENCY = pd.Timedelta(days=30)
        # Stored as hourly analysis-like best-guess: hourly production_datetime, leadtime 0.
        # (The forecast reference is 3-hourly and leadtimes 1/2/3 h are collapsed into the
        # hourly event time = reference + leadtime.)
        PRODUCTION_FREQUENCY = pd.Timedelta(hours=1)
        LEADTIMES = pd.timedelta_range('0h', '0h', freq='1h')

        # Only used to round KML bounding boxes; the stored grid keeps real 2-D coords.
        PIXEL_SIZE = 0.05

        SOURCE_PARALLEL_TRANSFERS = 3

        # Local/cloud (grib) files are keyed by the forecast REFERENCE month (the grib that
        # physically contains each hour) via the per-row {{ref_year}}/{{ref_ym}} tokens, since
        # hour 00:00 of a month is produced by the previous month's 21:00 forecast. Storage is
        # keyed by the EVENT (calendar) month so the .nct files are natural calendar months.
        CLOUD_TEMPLATE = 'CERRA_{self._variable_upper}/{{ref_year}}/cerra_{self._variable}_{{ref_ym}}.zip'
        LOCAL_PATH_TEMPLATE = 'CERRA_{self._variable_upper}/{{ref_year}}/cerra_{self._variable}_{{ref_ym}}.zip'
        STORAGE_PATH_TEMPLATE = 'CERRA_{self._variable_upper}/cerra_{self._variable}_{self._zone}/%Y/tethys_cerra_{self._variable}_%Y.%m.01.nct'

        VARIABLE_DICT = dict(
            tp='total_precipitation',
            t2m='2m_temperature',
            sd='snow_depth_water_equivalent',
        )

        # No ERA5-style cross-file de-accumulation: tp is de-accumulated within a single
        # forecast (its leadtimes arrive together), so nothing is cumulative here.
        CUMULATIVE = dict(tp=False, t2m=False, sd=False)

    def __init__(self, *args, **kwargs):
        # Skip ERA5.__init__ (which precomputes previous_local_file for the cumulative
        # boundary path we never take). BaseTask.__init__ builds the index we need.
        BaseTask.__init__(self, *args, **kwargs)

    # ------------------------------------------------------------------ tokens
    @staticmethod
    def _ref_time(event_datetime):
        '''
        Forecast reference time for an hourly event: the previous 3-hourly mark (leadtimes
        are 1..3 h, so hour 00:00 maps back to 21:00 of the previous day). Vectorized over a
        pandas datetime Series.
        '''
        return (event_datetime - pd.Timedelta('1h')).dt.floor('3h')

    def populate(self, *args, **kwargs):
        # Inject the reference-month tokens used by the local/cloud (grib) path templates.
        ref = lambda x: self._ref_time(x['production_datetime'])
        additional_columns = {
            'ref_year': lambda x: ref(x).dt.strftime('%Y'),
            'ref_ym': lambda x: ref(x).dt.strftime('%Y.%m'),
        }
        return super().populate(additional_columns=additional_columns, *args, **kwargs)

    # -------------------------------------------------------------- grib reading
    @staticmethod
    def _read_file(grib_file: str, variable: str = '', bounded=False, bounding_box=None) -> dict:
        '''
        Reads one CERRA grib on its native 2-D Lambert grid.

        The cfgrib/eccodes short name for a CERRA field is NOT our short key (e.g. tp is
        decoded as ``TOT_PREC``), so the single data variable is selected by position -- the
        ``variable`` argument is kept only for signature compatibility with ERA5.

        Longitudes are remapped to -180..180 (WGS84) to match the KML bounding boxes.
        When ``bounded`` the region is cropped in (y,x) index space BEFORE compute() so
        only the small region is materialised (the full domain is ~1069x1069x240x3).
        Returns dict with 2-D ``latitudes``/``longitudes`` and data shaped (time, step, y, x).
        '''
        data = {}
        with xr.open_dataset(grib_file, engine='cfgrib', indexpath='', chunks={'time': 24}) as ds:
            variable_list = list(ds.data_vars)
            if len(variable_list) != 1:
                raise Exception(f'CERRA grib should have exactly one data variable, found {variable_list}.')
            data_var = variable_list[0]

            lat2d = np.asarray(ds['latitude'].values)
            lon2d = np.asarray(ds['longitude'].values)
            lon2d = np.where(lon2d > 180, lon2d - 360, lon2d)

            ds_ = ds
            if bounded and bounding_box:
                latr = np.round(lat2d, 6)
                lonr = np.round(lon2d, 6)
                inside = ((latr >= round(bounding_box['south'], 6)) &
                          (latr <= round(bounding_box['north'], 6)) &
                          (lonr >= round(bounding_box['west'], 6)) &
                          (lonr <= round(bounding_box['east'], 6)))
                if not inside.any():
                    raise Exception('Bounding box does not intersect the CERRA domain.')
                yy, xx = np.nonzero(inside)
                y0, y1 = int(yy.min()), int(yy.max()) + 1
                x0, x1 = int(xx.min()), int(xx.max()) + 1
                ds_ = ds_.isel(y=slice(y0, y1), x=slice(x0, x1))
                lat2d = lat2d[y0:y1, x0:x1]
                lon2d = lon2d[y0:y1, x0:x1]

            data['latitudes'] = lat2d
            data['longitudes'] = lon2d
            data['production_datetime'] = np.atleast_1d(ds_['time'].data)
            data['steps'] = np.atleast_1d(ds_['step'].data)
            # (time, step, y, x) -- only the cropped region is computed.
            data['data'] = ds_[data_var].compute().values

        return data

    def read_local(self, local_file: str, bounded=True) -> MeteoRaster:
        '''
        Returns a MeteoRaster of best-guess hourly CERRA data.

        Hourly analysis-like product: production_datetime = event time (reference + step),
        leadtimes = [0], 2-D WGS84 lat/lon. tp is de-accumulated along the step axis (per
        reference time) before flattening; t2m/sd are instantaneous. No parquet boundary logic.
        '''
        self.diag(f'            Reading "{local_file}" ({self.__class__.__name__})', 1)

        if not Path(local_file).exists():
            raise Exception('Local file does not exit.')

        folder = self._ensure_unpacked(local_file)
        grib_files = list(folder.glob('*.grib'))
        if len(grib_files) != 1:
            raise ValueError(f'Expected exactly one .grib file in {local_file}, found {len(grib_files)}.')
        grib_file = grib_files[0]

        try:
            data = self._read_file(grib_file, self._variable, bounded=bounded, bounding_box=self.storage_bounding_box)
        except Exception as ex:
            raise Exception(f'{str(ex)} ({self.__class__.__name__}).')

        arr = data['data']  # (time, step, y, x)
        if arr.ndim != 4:
            raise Exception(f'Unexpected CERRA data shape {arr.shape} ({self.__class__.__name__}).')

        if self._variable == 'tp':
            # Accumulated from forecast start -> hourly increment (kg/m2/h = mm/hr).
            # diff along the step axis, per reference time (each time is a fresh forecast).
            arr = np.diff(arr, n=1, axis=1, prepend=0)
            # Clip tiny negatives from GRIB packing noise in the accumulation.
            arr = np.clip(arr, 0.0, None)
            units = 'mm/hr'
        elif self._variable == 't2m':
            arr = arr - 273.15
            units = 'C'
        elif self._variable == 'sd':
            # Snow depth water equivalent: kg/m2 = mm (instantaneous).
            units = 'mm'
        else:
            units = 'unknown'

        # Collapse (reference, step) into an hourly event series with a single leadtime 0.
        # event_time = reference + step; row-major ravel keeps data and times aligned.
        refs = np.asarray(data['production_datetime'])          # (time,)
        steps = np.atleast_1d(data['steps'])                    # (step,)
        event_times = (refs[:, None] + steps[None, :]).ravel()  # (time*step,)

        ny, nx = arr.shape[-2], arr.shape[-1]
        arr = arr.reshape(arr.shape[0] * arr.shape[1], ny, nx)  # (time*step, y, x)
        # (event, ensemble=1, leadtime=1, y, x)
        arr = arr[:, None, None, :, :]

        out = dict(
            data=arr,
            latitudes=data['latitudes'],
            longitudes=data['longitudes'],
            production_datetime=event_times,
            leadtimes=np.array([pd.Timedelta('0h')]),
        )

        mr = MeteoRaster(out, units=units, variable=self._variable, verbose=False)
        mr.trim()
        return mr

    # --------------------------------------------------------------- downloads
    @staticmethod
    def _extract_gribs(downloaded: Path, dest: Path):
        '''
        Extracts the grib(s) from one CDS response. Returns (grib_paths, source_was_zip).
        Handles both the CDS zip case and the bare-grib fallback.
        '''
        dest.mkdir(parents=True, exist_ok=True)
        downloaded = Path(downloaded)
        try:
            with ZipFile(downloaded, 'r') as z:
                z.extractall(dest)
        except BadZipFile:
            bare = dest / (downloaded.stem + '.grib')
            shutil.copyfile(downloaded, bare)
            return [bare], False
        gribs = sorted(dest.glob('*.grib'))
        if not gribs:
            raise OSError(f'CDS response {downloaded.name} contained no .grib.')
        return gribs, True

    def _validate_grib(self, grib_file: Path) -> None:
        '''
        Cheap coord-only validation: one data variable, 2-D Lambert lat/lon, >=3 steps
        (the requested leadtimes 1/2/3 h). Never materialises the data array.
        '''
        with xr.open_dataset(grib_file, engine='cfgrib', indexpath='') as ds:
            variable_list = list(ds.data_vars)
            if len(variable_list) != 1:
                raise OSError(f'Expected exactly one data variable, found {variable_list}.')
            if 'latitude' not in ds.coords or ds['latitude'].ndim != 2:
                raise OSError('Expected 2-D latitude/longitude (Lambert grid).')
            steps = np.atleast_1d(ds['step'].data)
            if len(steps) < 3:
                raise OSError(f'Grib has {len(steps)} step(s) (<3).')

    def _download_cds_chunk(self, variables):
        '''
        Downloads one month. ``variables`` is (request_options, local_path). The response
        is validated and kept verbatim as the local zip (only re-zipped if CDS returns a
        bare grib).
        '''
        options, local_path = variables
        local_path_ = Path(local_path)
        c = self._cds_client()
        workdir = Path(tempfile.mkdtemp(prefix='cerra_dl_'))
        try:
            dl = workdir / 'download'
            c.retrieve('reanalysis-cerra-single-levels', options).download(str(dl))
            gribs, was_zip = self._extract_gribs(dl, workdir / 'grib')

            self._validate_grib(gribs[0])

            local_path_.parent.mkdir(parents=True, exist_ok=True)
            if local_path_.exists():
                local_path_.unlink()

            if was_zip:
                shutil.move(str(dl), str(local_path_))
            else:
                with ZipFile(local_path_, 'w', compression=ZIP_DEFLATED) as z:
                    z.write(gribs[0], arcname=f'{local_path_.stem}.grib')

            return ((True, local_path))
        except Exception as ex:
            print(f'Download failed ({local_path_.name}): {ex}')
            return ((False, local_path))
        finally:
            shutil.rmtree(workdir, ignore_errors=True)

    def _download_from_source(self) -> bool:
        '''
        Downloads missing months directly from CDS (one forecast request per month, full
        European domain, all three variables sharing the same request shape).

        Returns True if downloads were made.
        '''
        self.diag('    Download from source...', 1)

        to_retrieve = self.data_index.loc[~self.data_index['data_exists'], :]
        files_to_download = to_retrieve['local_file'].unique()
        if len(files_to_download) == 0:
            self.diag('        Nothing to download.', 1)
            return False

        # Never request days beyond what is published (defensive; date_to is the real bound).
        upper_day = self.last_production_datetime.normalize()

        info = []
        for local_path in files_to_download:
            block_rows = self.data_index.loc[self.data_index['local_file'] == local_path]
            # A local file is keyed by the forecast REFERENCE month, so derive the month to
            # request from the reference time (not the event time -- they differ at 00:00).
            ref = self._ref_time(block_rows['production_datetime']).min()
            month_start = ref.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            month_end = month_start + pd.offsets.MonthEnd(0)
            upper = min(month_end, upper_day)
            day_index = pd.date_range(month_start, upper, freq='D')
            if len(day_index) == 0:
                continue

            options = {'variable': [self._variable_dict[self._variable]],
                       'level_type': 'surface_or_atmosphere',
                       'data_type': ['reanalysis'],
                       'product_type': 'forecast',
                       'year': f'{month_start.year}',
                       'month': f'{month_start.month:02d}',
                       'day': [f'{d:02d}' for d in day_index.day],
                       'time': [f'{h:02d}:00' for h in range(0, 24, 3)],
                       'leadtime_hour': ['1', '2', '3'],
                       'data_format': 'grib',
                       'download_format': 'zip',
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
                futures = [executor.submit(self._download_cds_chunk, i) for i in info[::-1]]
                for future in as_completed(futures):
                    state, local_path_ = future.result()
                    if state:
                        self.data_index.loc[self.data_index['local_file'] == local_path_, 'local_file_exists'] = True
                        downloaded = True
                        self.diag('        ' + monitor.mark_success(local_path_), 1)
                    else:
                        self.diag(f'        Download failed for {Path(local_path_).name}.', 1)

        return downloaded


# creates regional classes such as CERRA_TP_BELGIUM, CERRA_T2M_SWITZERLAND, CERRA_SD_IBERIA, ...
create_kml_classes(CERRA, {'VARIABLE': ['tp', 't2m', 'sd']})

if __name__ == '__main__':
    import matplotlib.pyplot as plt
    plt.ion()

    kwargs = dict(download_from_origin=False,
                #   source_parallel_transfers=1,
                  date_from='2025-09-01',
                  date_to='2026-04-30 23:59:59')
    # task = CERRA_TP_IBERIA(**kwargs)  # noqa: F821  (created at runtime)
    # task = CERRA_T2M_IBERIA(**kwargs)  # noqa: F821  (created at runtime)
    # task = CERRA_SD_IBERIA(**kwargs)  # noqa: F821  (created at runtime)
    task.update()

    # docker-compose run --rm tethys-tasks CERRA_TP_SWITZERLAND update --class_kwargs "{\"download_from_origin\": \"True\", \"date_from\": \"'2026-04-01'\", \"date_to\": \"'2026-05-01'\"}"

    pass
