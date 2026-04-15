"""Météo-France AROME 0.025° variable tasks.

Classes (each expands into zone-specific subclasses via create_kml_classes):
  AROME_0025_T2M  - 2-metre air temperature  [K]
  AROME_0025_TP   - total precipitation, 1-hour accumulation  [kg m-2]
  AROME_0025_SWE  - snow depth water equivalent  [kg m-2]

Authentication:
    METEOFRANCE_API_KEY — API key sent in the apikey header
"""
from tethys_tasks import BaseTask, CaptureNewVariables, DownloadMonitor, create_kml_classes
from concurrent.futures import ThreadPoolExecutor, as_completed
import numpy as np
import os
from pathlib import Path
import pandas as pd
import shutil
import subprocess
import tempfile
import time
import datetime
import xarray as xr
from zipfile import ZipFile
from meteoraster import MeteoRaster


_WCS_BASE = (
    'https://public-api.meteofrance.fr/public/arome/1.0/wcs/'
    'MF-NWP-HIGHRES-AROME-0025-FRANCE-WCS/GetCoverage'
    '?service=WCS&version=2.0.1'
)
_GRIB_CONTENT_TYPES = {'application/octet-stream', 'application/wmo-grib'}

CLOUD_TEMPLATE_ = 'AROME_0025_FR/{self._variable_upper}/%Y/%m/arome_0025_fr_{self._variable_lower}_%Y%m%dT%H.zip'
LOCAL_PATH_TEMPLATE_ = CLOUD_TEMPLATE_
STORAGE_PATH_TEMPLATE_ = 'AROME_0025_FR/arome_{self._variable}_{self._zone}/%Y/%m/tethys_arome_0025_fr_{self._variable_lower}_%Y%m%d.nct'

class AROME_0025_FR_Base(BaseTask):
    """Shared WCS download machinery for all AROME 0.025° variable tasks."""

    with CaptureNewVariables() as _AROME_0025_FR_Base_VARIABLES:
        PUBLICATION_LATENCY  = pd.Timedelta(hours=6)
        PUBLICATION_MEMORY   = pd.Timedelta(hours=18)
        PRODUCTION_FREQUENCY = pd.Timedelta(hours=3)
        LEADTIMES            = pd.timedelta_range('0h', '51h', freq='1h')
        REQUEST_LEADTIME_OFFSET = pd.Timedelta('0h')
        PIXEL_SIZE           = 0.025
        DOWNLOAD_RETRIES     = 1
        DOWNLOAD_RETRY_WAIT  = 30

        SOURCE_PARALLEL_TRANSFERS = 1
        STORAGE_SEARCH_WINDOW     = pd.DateOffset(days=10)
        ASSUME_LOCAL_COMPLETE     = False

        API_KEY   = os.environ.get('METEOFRANCE_API_KEY', '') or os.environ.get('METEOFRANCE_API_TOKEN', '')
        API_TOKEN = API_KEY

        DATE_FROM     = (pd.Timestamp.now(datetime.timezone.utc) - pd.Timedelta('1D')).strftime('%Y-%m-%d %H:%M:%S')
        FAIL_IF_OLDER = pd.Timedelta('9h')

    @staticmethod
    def _run_str(dt: pd.Timestamp) -> str:
        """WCS coverage ID run timestamp — colons replaced by dots in time part."""
        return pd.Timestamp(dt).strftime('%Y-%m-%dT%H.%M.%SZ')

    def _coverage_id(self, prod_dt: pd.Timestamp) -> str:
        cid = f'{self._wcs_parameter}___{self._run_str(prod_dt)}'
        if self._wcs_period:
            cid += f'_{self._wcs_period}'
        return cid

    def _get_api_key(self) -> str:
        """Read the API key fresh from the environment."""
        return (
            os.environ.get('METEOFRANCE_API_KEY', '')
            or os.environ.get('METEOFRANCE_API_TOKEN', '')
            or getattr(self, '_api_key', '')
            or getattr(self, '_api_token', '')
            or ''
        )

    @staticmethod
    def _get_curl_executable() -> str | None:
        return shutil.which('curl.exe') or shutil.which('curl')

    @staticmethod
    def _parse_headers(raw_headers: bytes) -> tuple[str, str]:
        text = raw_headers.decode('utf-8', errors='replace').replace('\r\n', '\n')
        blocks = [block.strip() for block in text.split('\n\n') if block.strip()]
        if not blocks:
            return '', ''

        lines = blocks[-1].splitlines()
        status_line = lines[0] if lines else ''
        content_type = next(
            (line.split(':', 1)[1].strip() for line in lines if line.lower().startswith('content-type:')),
            '',
        )
        return status_line, content_type

    @staticmethod
    def _read_preview(path: Path, size: int = 512) -> bytes:
        with path.open('rb') as fh:
            return fh.read(size)

    @staticmethod
    def _preview_text(body: bytes) -> str:
        return body.decode('utf-8', errors='replace')

    def _wcs_url(self, prod_dt: pd.Timestamp, valid_dt: pd.Timestamp) -> str:
        valid = pd.Timestamp(valid_dt).strftime('%Y-%m-%dT%H:%M:%SZ')
        url   = f'{_WCS_BASE}&coverageid={self._coverage_id(prod_dt)}&subset=time({valid})'
        if self._wcs_height:
            url += f'&subset=height({self._wcs_height})'
        url += '&format=application/wmo-grib'
        return url

    def _request_valid_dt(self, prod_dt: pd.Timestamp, leadtime: pd.Timedelta) -> pd.Timestamp:
        return pd.Timestamp(prod_dt) + pd.Timedelta(leadtime) + self._request_leadtime_offset

    def _member_name(self, prod_dt: pd.Timestamp, leadtime: pd.Timedelta) -> str:
        hours = int(pd.Timedelta(leadtime).total_seconds() // 3600)
        return f'arome_0025_{self._variable}_{prod_dt:%Y%m%dT%H}_{hours:03d}h.grib2'

    def _download_one_status(self, url: str, dest: Path) -> str:
        dest.parent.mkdir(parents=True, exist_ok=True)
        curl_exe = self._get_curl_executable()
        if not curl_exe:
            self.diag('        Download error: curl is required to fetch AROME data.', 1)
            return 'Failed'

        api_key = self._get_api_key().strip()
        if not api_key:
            self.diag('        Download error: METEOFRANCE_API_KEY is required to fetch AROME data.', 1)
            return 'Failed'

        retries = max(int(self._download_retries), 0)
        wait_seconds = max(float(self._download_retry_wait), 0.0)
        last_error = None

        for attempt in range(retries + 1):
            temp_path = None
            try:
                fd, tmp = tempfile.mkstemp(prefix=dest.stem + '.', suffix='.part', dir=dest.parent)
                os.close(fd)
                temp_path = Path(tmp)

                cmd = [
                    curl_exe,
                    '-X', 'GET',
                    '-sS',
                    '--max-time', '120',
                    '-D', '-',
                    '-o', str(temp_path),
                    url,
                    '-H', 'accept: */*',
                    '-H', f'apikey: {api_key}',
                ]
                result = subprocess.run(cmd, capture_output=True)
                status_line, content_type = self._parse_headers(result.stdout)
                body_head = self._read_preview(temp_path)

                if result.returncode != 0:
                    stderr = result.stderr.decode('utf-8', errors='replace').strip()
                    raise RuntimeError(f'curl exited with code {result.returncode}: {stderr or status_line}')

                status_parts = status_line.split()
                status_code = int(status_parts[1]) if len(status_parts) >= 2 and status_parts[1].isdigit() else None
                if status_code is not None and not (200 <= status_code < 300):
                    raise RuntimeError(
                        f'HTTP {status_code}. Content-Type: {content_type!r}. '
                        f'Body preview: {self._preview_text(body_head)!r}'
                    )

                if not (body_head.startswith(b'GRIB') or content_type in _GRIB_CONTENT_TYPES):
                    raise RuntimeError(
                        f'API returned unexpected content instead of GRIB2. '
                        f'Status: {status_line or "<missing>"}. '
                        f'Content-Type: {content_type!r}. '
                        f'Body preview: {self._preview_text(body_head)!r}'
                    )

                temp_path.replace(dest)
                return 'Downloaded'
            except Exception as ex:
                last_error = ex
                if temp_path is not None and temp_path.exists():
                    temp_path.unlink()
                if attempt < retries:
                    self.diag(
                        f'        Download attempt {attempt + 1}/{retries + 1} failed; retrying in {wait_seconds:g}s.',
                        1,
                    )
                    time.sleep(wait_seconds)

        self.diag(f'        Download error: {last_error}', 1)
        if isinstance(last_error, RuntimeError) and str(last_error).startswith('HTTP 404'):
            return 'Not found'
        return 'Failed'

    def _download_one(self, url: str, dest: Path) -> bool:
        return self._download_one_status(url, dest) == 'Downloaded'

    def _download_member(self, prod_dt: pd.Timestamp, leadtime: pd.Timedelta, tmp_dir: str) -> tuple[str, Path, pd.Timedelta]:
        dest = Path(tmp_dir) / self._member_name(prod_dt, leadtime)
        valid_dt = self._request_valid_dt(prod_dt, leadtime)
        status = self._download_one_status(self._wcs_url(prod_dt, valid_dt), dest)
        return status, dest, leadtime

    def _download_from_source(self) -> bool:
        self.diag('    Download from source...', 1)

        cutoff = (
            pd.Timestamp.now(datetime.timezone.utc).tz_localize(None)
            - self._publication_memory - self._publication_latency
        )
        pending = (
            self.data_index
            .loc[~self.data_index['local_file_complete'], ['production_datetime', 'local_file']]
            .drop_duplicates()
            .loc[lambda df: df['production_datetime'] >= cutoff]
        )
        if pending.empty:
            self.diag('        Nothing to download.', 1)
            return False

        downloaded = False
        self.diag(f'        Downloading ({self._source_parallel_transfers} threads).', 1)
        for _, row in pending.sort_values('production_datetime').iterrows():
            prod_dt    = row['production_datetime']
            local_file = Path(row['local_file'])
            self.diag(f'        {self._variable.upper()} {prod_dt:%Y-%m-%d %H:%M}...', 1)

            with tempfile.TemporaryDirectory(prefix='arome_0025_') as tmp_dir:
                staged, failed = {}, 0
                with DownloadMonitor() as monitor:
                    with ThreadPoolExecutor(max_workers=self._source_parallel_transfers) as executor:
                        leadtime_iter = iter(self._leadtimes)
                        futures = {}

                        for _ in range(self._source_parallel_transfers):
                            leadtime = next(leadtime_iter, None)
                            if leadtime is None:
                                break
                            futures[executor.submit(self._download_member, prod_dt, leadtime, tmp_dir)] = leadtime

                        while futures:
                            future = next(as_completed(futures))
                            status, dest, leadtime = future.result()
                            futures.pop(future)
                            if status == 'Downloaded':
                                staged[leadtime] = dest
                                self.diag('        ' + monitor.mark_success(dest), 1)
                                next_leadtime = next(leadtime_iter, None)
                                if next_leadtime is not None:
                                    futures[executor.submit(self._download_member, prod_dt, next_leadtime, tmp_dir)] = next_leadtime
                            elif status == 'Not found':
                                failed += 1
                                self.diag(f'        Missing {self._member_name(prod_dt, leadtime)}.', 1)
                            else:
                                failed += 1
                                self.diag(f'        Failed {self._member_name(prod_dt, leadtime)}.', 1)

                if failed:
                    self.diag(f'        Incomplete run ({len(staged)}/{len(self._leadtimes)} lead times).', 1)
                    break

                local_file.parent.mkdir(parents=True, exist_ok=True)
                fd_z, tmp_z = tempfile.mkstemp(
                    prefix=local_file.stem + '.', suffix='.part', dir=local_file.parent
                )
                os.close(fd_z)
                tmp_zip = Path(tmp_z)
                try:
                    with ZipFile(tmp_zip, 'w') as zf:
                        for _, f in sorted(staged.items()):
                            zf.write(f, arcname=f.name)
                    shutil.move(str(tmp_zip), str(local_file))
                    self.diag(f'        Saved {local_file.name}.', 1)
                    downloaded = True
                finally:
                    if tmp_zip.exists():
                        tmp_zip.unlink()

        if downloaded:
            self._check_existing_data(stored=False, cloud=False)
        return downloaded

    def read_local_completeness(self, local_file: str) -> pd.Series:
        '''
        Returns completeness of the local file based on the presence of leadtime files.
        Not thoroughly reliable, but gives a hint on whether a file is still being written to or not.
        '''

        empty = pd.MultiIndex.from_arrays([[], []], names=['production_datetime', 'leadtime'])
        path  = Path(local_file)
        if not path.exists():
            return pd.Series(dtype=bool, index=empty)
        rows = self.data_index.loc[self.data_index['local_file'] == local_file, 'production_datetime']
        if rows.empty:
            return pd.Series(dtype=bool, index=empty)
        prod_dt = rows.iloc[0]
        with ZipFile(path) as zf:
            members = {Path(n).name for n in zf.namelist() if not n.endswith('/')}
        valid = [(prod_dt, lt) for lt in self._leadtimes if self._member_name(prod_dt, lt) in members]
        if not valid:
            return pd.Series(dtype=bool, index=empty)
        return pd.Series(True, index=pd.MultiIndex.from_tuples(valid, names=['production_datetime', 'leadtime']))

    def read_local(self, local_file: str) -> MeteoRaster:
        """Read a local AROME 0.025° ZIP archive and return a MeteoRaster."""

        self.diag(f'            Reading "{local_file}" ({self.__class__.__name__})', 1)

        with tempfile.TemporaryDirectory(prefix='arome_0025_read_') as tmp_dir:
            tmp_path = Path(tmp_dir)
            with ZipFile(local_file, 'r') as zf:
                zf.extractall(tmp_path)

            grib_files = sorted(tmp_path.glob('*.grib2'))
            if not grib_files:
                raise RuntimeError(f'No .grib2 files found in {local_file}.')

            # Determine grid dimensions from first file
            with xr.open_dataset(str(grib_files[0]), engine='cfgrib', indexpath='') as ds:
                lats = ds.latitude.data
                lons = ds.longitude.data

            rows = self.data_index.loc[self.data_index['local_file'] == local_file]
            if rows.empty:
                raise RuntimeError(f'No index entries for {local_file}.')
            prod_dt = rows['production_datetime'].iloc[0]

            leadtimes = self._leadtimes
            full_data = np.full(
                (1, 1, len(leadtimes), len(lats), len(lons)), np.nan, dtype=np.float32
            )
            lead_map = {lt: i for i, lt in enumerate(leadtimes)}

            for gf in grib_files:
                # Member name ends with e.g. '_003h.grib2' — parse hours from stem
                try:
                    hours = int(gf.stem.split('_')[-1].rstrip('h'))
                    lt = pd.Timedelta(hours=hours)
                except (ValueError, IndexError):
                    continue
                if lt not in lead_map:
                    continue

                with xr.open_dataset(str(gf), engine='cfgrib', indexpath='') as ds:
                    vname = next(iter(ds.data_vars))
                    vals  = ds[vname].data
                    if vals.ndim > 2:
                        vals = vals.reshape(vals.shape[-2], vals.shape[-1])
                    full_data[0, 0, lead_map[lt], :, :] = vals.astype(np.float32)

        # Variable-specific unit conversion
        if self._variable == 't2m':
            full_data -= 273.15
            units = 'C'
        elif self._variable == 'tp':
            units = 'mm/h'   # WCS delivers 1-hour accumulation in kg m-2 == mm
        elif self._variable == 'swe':
            units = 'mm'     # kg m-2 == mm
        else:
            units = 'unknown'

        mr = MeteoRaster(
            data=dict(
                data=full_data,
                latitudes=lats,
                longitudes=lons,
                production_datetime = np.array([pd.Timestamp(prod_dt).to_datetime64()]),
                leadtimes=leadtimes,
            ),
            units=units,
            variable=self._variable,
            verbose=False,
        )
        mr.trim()
        return mr
    

class AROME_0025_T2M(AROME_0025_FR_Base):
    """2-metre air temperature from AROME 0.025°."""

    with CaptureNewVariables() as _AROME_0025_T2M_VARIABLES:
        VARIABLE      = 't2m'
        WCS_PARAMETER = 'TEMPERATURE__SPECIFIC_HEIGHT_LEVEL_ABOVE_GROUND'
        WCS_HEIGHT    = 2      # metres above ground
        WCS_PERIOD    = ''     # instantaneous field

        CLOUD_TEMPLATE = CLOUD_TEMPLATE_
        LOCAL_PATH_TEMPLATE = LOCAL_PATH_TEMPLATE_
        STORAGE_PATH_TEMPLATE = STORAGE_PATH_TEMPLATE_


class AROME_0025_TP(AROME_0025_FR_Base):
    """Total precipitation, 1-hour accumulation, from AROME 0.025°."""

    with CaptureNewVariables() as _AROME_0025_TP_VARIABLES:
        VARIABLE      = 'tp'
        LEADTIMES     = pd.timedelta_range('0h', '50h', freq='1h')
        REQUEST_LEADTIME_OFFSET = pd.Timedelta('1h')
        WCS_PARAMETER = 'TOTAL_WATER_PRECIPITATION__GROUND_OR_WATER_SURFACE'
        WCS_HEIGHT    = 0      # surface; 0 → no height subset in URL
        WCS_PERIOD    = 'PT1H'

        CLOUD_TEMPLATE = CLOUD_TEMPLATE_
        LOCAL_PATH_TEMPLATE = LOCAL_PATH_TEMPLATE_
        STORAGE_PATH_TEMPLATE = STORAGE_PATH_TEMPLATE_


class AROME_0025_SWE(AROME_0025_FR_Base):
    """Snow depth water equivalent from AROME 0.025°."""

    with CaptureNewVariables() as _AROME_0025_SWE_VARIABLES:
        VARIABLE      = 'swe'

        WCS_PARAMETER = 'WATER_EQUIVALENT_ACCUMULATED_SNOW__GROUND_OR_WATER_SURFACE'
        WCS_HEIGHT    = 0
        WCS_PERIOD    = ''

        CLOUD_TEMPLATE = CLOUD_TEMPLATE_
        LOCAL_PATH_TEMPLATE = LOCAL_PATH_TEMPLATE_
        STORAGE_PATH_TEMPLATE = STORAGE_PATH_TEMPLATE_

class AROME_0025_T2M_BELGIUM(AROME_0025_T2M):
    """2-metre air temperature from AROME 0.025°."""

    with CaptureNewVariables() as _AROME_0025_T2M_BELGIUM_VARIABLES:
        ZONE = 'BELGIUM'
        STORAGE_KML = 'tethys_tasks/resources/belgium.kml'

class AROME_0025_TP_BELGIUM(AROME_0025_TP):
    """Total precipitation from AROME 0.025°."""

    with CaptureNewVariables() as _AROME_0025_TP_BELGIUM_VARIABLES:
        ZONE = 'BELGIUM'
        STORAGE_KML = 'tethys_tasks/resources/belgium.kml'

class AROME_0025_SWE_BELGIUM(AROME_0025_SWE):
    """Snow depth water equivalent from AROME 0.025°."""

    with CaptureNewVariables() as _AROME_0025_SWE_BELGIUM_VARIABLES:
        ZONE = 'BELGIUM'
        STORAGE_KML = 'tethys_tasks/resources/belgium.kml'

if __name__ == '__main__':
    import matplotlib.pyplot as plt
    plt.ion()

    from dotenv import load_dotenv
    load_dotenv()

    kwargs = dict(download_from_source=True, date_from='2026-04-14')

    task = AROME_0025_T2M_BELGIUM(**kwargs)
    # task = AROME_0025_TP_BELGIUM(**kwargs)
    # task = AROME_0025_SWE_BELGIUM(**kwargs)
    task.update()

    # stored_files = task.data_index['stored_file'].unique()
    # mr = MeteoRaster.load(stored_files[-1])
    # mr.plot_mean(coastline=True, borders=True)
    # mr.get_values_from_latlon_by_event(mr.get_values_from_latlon(50.5, 4.5)).plot(marker='o')
    pass
