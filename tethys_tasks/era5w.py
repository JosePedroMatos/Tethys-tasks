'''
ERA5W -- world ERA5 Land download in month-aligned 4-day blocks.

Motivation
----------
Full-world ERA5 Land local files are downloaded/extracted/read one *month* at a
time (see era5.ERA5). A world month of hourly data is enormous and causes memory,
disk and CDS-request-size problems. ERA5W keeps everything ERA5 does for reading,
cumulative handling and storage, but changes only the *local/cloud step* to a
4-day block (~8x smaller per file), always in world mode.

Design decisions
-----------------
* Blocks are 4 days aligned to the start of each month: days 1-4, 5-8, ..., 25-28,
  29-end. The last block of a month is 1-3 days. Because a block never crosses a
  month boundary, every CDS request is a single (year, month, day-list) call --
  no cross-month splitting or grib concatenation needed.
* World only: the `era5_local_world` flag is bypassed entirely (area is never sent).
* Storage is inherited unchanged from ERA5 (monthly, region-cropped, `era5_*`
  paths). Storage is independent of the local chunking, so ERA5W simply feeds the
  same stored .nct product through a different ingestion path. Only the local and
  cloud world paths carry the `era5w` naming.
'''

from tethys_tasks import CaptureNewVariables, create_kml_classes, DownloadMonitor
from tethys_tasks.era5 import ERA5
import pandas as pd
import numpy as np
import xarray as xr
from pathlib import Path
import tempfile
import shutil
import random
import string
from zipfile import ZipFile, BadZipFile, ZIP_DEFLATED
from concurrent.futures import ThreadPoolExecutor, as_completed


class ERA5W(ERA5):
    '''
    World ERA5 Land in month-aligned 4-day local/cloud blocks.
    Inherits reading, cumulative handling, unpack cache and storage from ERA5.
    '''

    with CaptureNewVariables() as _ERA5W_VARIABLES: #It is essential that the format of the variable here is _CLASSnAME_VARIABLES
        # Local/cloud paths use the `era5w` naming and a 4-day block token. `%Y` is
        # resolved by strftime (block year == production year, since a block stays in
        # one month) and `{{floor_4_days}}` survives the __init__ `.format(self=self)`
        # as a single brace, resolved per row in populate() (same trick as GFS_025).
        # STORAGE_PATH_TEMPLATE is intentionally NOT redefined: storage stays the
        # monthly, region-cropped, `era5_*` product inherited from ERA5.
        CLOUD_TEMPLATE = 'ERA5W_{self._variable_upper}/era5w_{self._variable}_world/%Y/era5w_{self._variable}_{{floor_4_days}}.zip'
        LOCAL_PATH_TEMPLATE = 'ERA5W_{self._variable_upper}/era5w_{self._variable}_world/%Y/era5w_{self._variable}_{{floor_4_days}}.zip'

        # World only -- the flag is bypassed, this is just documentation/consistency.
        ERA5_LOCAL_WORLD = True

    # ------------------------------------------------------------------ tokens
    @staticmethod
    def _block_start_day(day):
        '''Month-aligned 4-day block start day for a day-of-month: 1,5,9,...,29.'''
        return ((day - 1) // 4) * 4 + 1

    def _floor_4_days(self, production_datetime):
        '''Vectorized 'YYYY.MM.DD' token of the block start for each timestamp.'''
        start_day = self._block_start_day(production_datetime.dt.day)
        return production_datetime.dt.strftime('%Y.%m.') + start_day.astype(int).astype(str).str.zfill(2)

    def _block_start(self, ts: pd.Timestamp) -> pd.Timestamp:
        '''Scalar block start (a midnight, same month) for one timestamp.'''
        return pd.Timestamp(year=ts.year, month=ts.month, day=self._block_start_day(ts.day))

    def populate(self, *args, **kwargs):
        # Inject the 4-day block token used by the local/cloud path templates.
        additional_columns = {'floor_4_days': lambda x: self._floor_4_days(x['production_datetime'])}
        return super().populate(additional_columns=additional_columns, *args, **kwargs)

    # --------------------------------------------------------------- downloads
    @staticmethod
    def _extract_gribs(downloaded: Path, dest: Path):
        '''
        Extracts the grib file from one CDS response.
        Returns (list_of_grib_paths, source_was_zip). Handles the CDS zip case and
        the fallback where CDS returns a bare grib instead of a zip.
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
        Cheaply validates a grib: one data variable and the expected 24 hourly steps.
        Reads only coordinates -- never materialises data, so it stays memory-safe
        even on a world block.
        '''
        with xr.open_dataset(grib_file, engine='cfgrib', indexpath='') as ds:
            variable_list = list(ds.data_vars)
            if len(variable_list) != 1:
                raise OSError(f'Expected exactly one data variable, found {variable_list}.')
            steps = np.atleast_1d(ds['step'].data)
            if self._variable not in ['sd'] and len(steps) < 24:
                raise OSError(f'Grib has {len(steps)} step(s) (<24).')

    def _download_cds_chunk(self, variables):
        '''
        Downloads one 4-day block. `variables` is (request_options, local_path).
        The block is a single in-month CDS request, so the returned zip is validated
        and kept verbatim as the local file (only re-zipped if CDS returns a bare grib).
        '''
        options, local_path = variables
        local_path_ = Path(local_path)
        c = self._cds_client()
        workdir = Path(tempfile.mkdtemp(prefix='era5w_dl_'))
        try:
            dl = workdir / 'download'
            c.retrieve('reanalysis-era5-land', options).download(str(dl))
            gribs, was_zip = self._extract_gribs(dl, workdir / 'grib')

            self._validate_grib(gribs[0])

            local_path_.parent.mkdir(parents=True, exist_ok=True)
            if local_path_.exists():
                local_path_.unlink()

            if was_zip:
                # Keep the CDS zip verbatim (avoids re-zipping GBs of grib).
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
        Downloads missing 4-day world blocks directly from CDS.

        Returns True if downloads were made.
        '''

        self.diag('    Download from source...', 1)

        to_retrieve = self.data_index.loc[~self.data_index['data_exists'], :]
        files_to_download = to_retrieve['local_file'].unique()
        if len(files_to_download) == 0:
            self.diag('        Nothing to download.', 1)
            return False

        # Never request days beyond what is published (avoids CDS errors on future days).
        upper_day = self.last_production_datetime.normalize()

        info = []
        for local_path in files_to_download:
            block_rows = self.data_index.loc[self.data_index['local_file'] == local_path]
            # Request the FULL block (not only the missing rows) so each file is a clean,
            # complete overwrite -- matching ERA5's whole-month semantics.
            block_start = self._block_start(block_rows['production_datetime'].iloc[0])
            # Clip to the month end (keeps the block in one month) and to published days.
            month_end = block_start + pd.offsets.MonthEnd(0)
            upper = min(block_start + pd.Timedelta(days=3), month_end, upper_day)
            day_index = pd.date_range(block_start, upper, freq='D')
            if len(day_index) == 0:
                continue

            # Single in-month request: year and month are scalars, day is the block list.
            options = {'data_format': 'grib',
                       'year': f'{block_start.year}',
                       'month': f'{block_start.month:02d}',
                       'day': [f'{d:02d}' for d in day_index.day],
                       'variable': [self._variable_dict[self._variable]],
                       'download_format': 'zip',
                       'time': [f'{h:02d}:00' for h in range(24)],
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


# creates regional classes such as ERA5W_TP_SWITZERLAND, ERA5W_T2M_CAUCASUS, etc...
create_kml_classes(ERA5W, {'VARIABLE': ['tp', 't2m', 'sd']})

if __name__ == '__main__':
    from meteoraster import MeteoRaster
    import matplotlib.pyplot as plt
    plt.ion()

    kwargs = dict(download_from_origin=True,
                  date_from='2020-01-01')
    # task = ERA5W_T2M_SWITZERLAND(**kwargs)
    # task = ERA5W_TP_SWITZERLAND(**kwargs)
    # task = ERA5W_T2M_ZAMBEZI(**kwargs)
    task = ERA5W_TP_SWITZERLAND(**kwargs)
    # task.retrieve()
    task.update()


    # mr = MeteoRaster.load(r'C:\tethys-tasks storage test\ERA5_T2M\era5_t2m_belgium\2026\tethys_era5_t2m_2026.01.01.nct')
    # mr.plot_mean(coastline=True, borders=True)

    # files = task.data_index['stored_file'].unique()
    # mr = MeteoRaster.load(files[-2])
    # mr.plot_mean(coastline=True, borders=True)
    # mr.get_values_from_latlon(47, 8).plot()

    # docker-compose run --rm tethys-tasks ERA5W_TP_SWITZERLAND update --class_kwargs "{\"download_from_origin\": \"True\", \"date_from\": \"'2026-05-01'\"}"

    pass
