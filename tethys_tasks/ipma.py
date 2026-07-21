from __future__ import annotations

from pathlib import Path
import os
import posixpath
import shutil
import tempfile
import datetime
from uuid import uuid4

import numpy as np
import pandas as pd
import xarray as xr
import cfgrib
from meteoraster import MeteoRaster

from tethys_tasks import BaseTask, CaptureNewVariables, DownloadMonitor, create_kml_classes


class IPMA(BaseTask):
    """IPMA deterministic "Predict" forecasts (single member) downloaded over SFTP.

    One GRIB file per production date and variable is published in the SFTP home directory,
    named ``Predict_YYYYMMDD_<name>.grib`` (``precipitacao`` for tp, ``t2m`` for t2m).
    The file holds a single field over an Atlantic/Iberia domain (28-46.7N, -37-0E @ 0.1 deg),
    120 forecast steps up to +240 h. Precipitation is stored per step (already de-accumulated),
    so it is only converted m -> mm; temperature is converted K -> C.
    """

    with CaptureNewVariables() as _IPMA_VARIABLES:
        PUBLICATION_LATENCY = pd.Timedelta(hours=9)
        PUBLICATION_MEMORY = pd.Timedelta(days=2)
        PRODUCTION_FREQUENCY = pd.Timedelta(hours=24)

        # Forecast steps present in the downloaded files (hourly to +84 h, 3-hourly to +144 h,
        # 6-hourly to +240 h). LEADTIMES == LEADTIMES_LOCAL: no post-processing drop is needed
        # because precipitation is already per-step (not cumulative).
        LEADTIMES = [pd.Timedelta(hours=h) for h in [*range(1, 85), *range(87, 145, 3), *range(150, 241, 6)]]
        LEADTIMES_LOCAL = [pd.Timedelta(hours=h) for h in [*range(1, 85), *range(87, 145, 3), *range(150, 241, 6)]]

        ASSUME_LOCAL_COMPLETE = True
        STORAGE_SEARCH_WINDOW = pd.DateOffset(days=14)
        SOURCE_PARALLEL_TRANSFERS = 1  # paramiko SFTP clients are not thread-safe; download sequentially.

        PIXEL_SIZE = 0.1
        VARIABLE = ''
        ZONE = 'atlantic'

        # SFTP access. Credentials come from the environment (loaded from .env by main.py).
        IPMA_SFTP_TARGET = os.getenv('IPMA_SFTP_TARGET')        # expected form: user@host
        IPMA_SFTP_PASSWORD = os.getenv('IPMA_SFTP_PASSWORD')
        SFTP_PORT = 22
        SFTP_DIR = os.getenv('IPMA_SFTP_DIR', '.')             # home/root directory by default
        REMOTE_CLEANUP_AGE = pd.Timedelta(hours=9)              # delete downloaded remote files older than this
        REMOTE_CLEANUP_WINDOW = pd.Timedelta(days=60)          # how far back to scan for stale remote files

        # Mapping between the internal variable name and the IPMA file/variable naming.
        REMOTE_VARIABLE_NAMES = {'tp': 'precipitacao', 't2m': 't2m'}
        REMOTE_FILE_TEMPLATE = 'Predict_%Y%m%d_{name}.grib'

        UNITS = {'t2m': 'C', 'tp': 'mm'}

        CLOUD_TEMPLATE = 'IPMA/{self._variable_upper}/%Y/%m/ipma_{self._variable_lower}_%Y%m%d.grib'
        LOCAL_PATH_TEMPLATE = 'IPMA/{self._variable_upper}/%Y/%m/ipma_{self._variable_lower}_%Y%m%d.grib'
        STORAGE_PATH_TEMPLATE = 'IPMA/ipma_{self._variable_lower}_{self._zone}/%Y/%m/tethys_ipma_{self._variable_lower}_%Y%m%d.nct'

        FAIL_IF_OLDER = pd.Timedelta(hours=36)
        DATE_FROM = (pd.Timestamp.now(datetime.timezone.utc) - pd.Timedelta('2D')).strftime('%Y-%m-%d %H:%M:%S')

    def _set_base_variables(self, cls, kwargs):
        super()._set_base_variables(cls, kwargs)

        self._leadtimes = [pd.Timedelta(lt) for lt in pd.to_timedelta(self._leadtimes)]
        leadtimes_local = getattr(self, '_leadtimes_local', None)
        if leadtimes_local is None:
            leadtimes_local = self._leadtimes
        self._leadtimes_local = [pd.Timedelta(lt) for lt in pd.to_timedelta(leadtimes_local)]

    # ------------------------------------------------------------------ naming helpers

    def _remote_filename(self, production_datetime: pd.Timestamp) -> str:
        name = self._remote_variable_names[self._variable]
        return pd.Timestamp(production_datetime).strftime(self._remote_file_template).format(name=name)

    def _remote_path(self, filename: str) -> str:
        remote_dir = (self._sftp_dir or '.').strip()
        if remote_dir in ('', '.'):
            return filename
        return posixpath.join(remote_dir, filename)

    def _local_file_for_date(self, production_datetime: pd.Timestamp) -> str:
        rel = pd.Timestamp(production_datetime).strftime(self._local_path_template)
        return self._local_storage_folder + '/' + rel

    # ------------------------------------------------------------------ validation helpers

    @staticmethod
    def _has_grib_magic(path: str) -> bool:
        try:
            with open(path, 'rb') as handle:
                return handle.read(4) == b'GRIB'
        except Exception:
            return False

    def _is_valid_grib(self, path: str) -> bool:
        '''A file is valid if it starts with the GRIB magic bytes and cfgrib can open it.'''
        try:
            if os.path.getsize(path) < 8 or not self._has_grib_magic(path):
                return False
            datasets = cfgrib.open_datasets(path, backend_kwargs={'indexpath': ''})
            ok = len(datasets) >= 1
            for ds in datasets:
                ds.close()
            return ok
        except Exception:
            return False

    def check_local(self, local_file: str) -> bool:
        return self._is_valid_grib(local_file)

    # ------------------------------------------------------------------ SFTP helpers

    def _parse_target(self) -> tuple[str, str]:
        target = (self._ipma_sftp_target or '').strip()
        if '@' not in target:
            raise RuntimeError(
                'IPMA_SFTP_TARGET must be set in the environment as "user@host" '
                f'(got {target!r}).'
            )
        user, host = target.rsplit('@', 1)
        return user, host

    def _sftp_connect(self):
        try:
            import paramiko
        except ImportError as ex:  # pragma: no cover - environment guard
            raise RuntimeError(
                'IPMA SFTP downloads require the "paramiko" package. Add it to environment.yml.'
            ) from ex

        if not self._ipma_sftp_password:
            raise RuntimeError('IPMA_SFTP_PASSWORD is not set in the environment.')

        user, host = self._parse_target()
        transport = paramiko.Transport((host, int(self._sftp_port)))
        transport.connect(username=user, password=self._ipma_sftp_password)
        sftp = paramiko.SFTPClient.from_transport(transport)
        return transport, sftp

    def _sftp_download_one(self, sftp, remote: str, local_file: Path) -> tuple[str, str]:
        '''Download a single remote file into local_file (atomic move via a temp .part file).'''
        local_file.parent.mkdir(parents=True, exist_ok=True)

        # Missing remote file: not yet published.
        try:
            sftp.stat(remote)
        except IOError:
            return 'Not found', remote

        fd, temp_name = tempfile.mkstemp(
            prefix=local_file.stem + '.',
            suffix=f'.{uuid4().hex}.part',
            dir=str(local_file.parent),
        )
        os.close(fd)
        temp_path = Path(temp_name)
        try:
            sftp.get(remote, str(temp_path))
            if not self._is_valid_grib(str(temp_path)):
                return 'Failed', 'downloaded file is not a valid GRIB'
            os.replace(str(temp_path), str(local_file))
            return 'Downloaded', str(local_file)
        except Exception as ex:
            return 'Failed', str(ex)
        finally:
            if temp_path.exists():
                temp_path.unlink()

    def _cleanup_remote(self, sftp) -> None:
        '''Delete remote files older than REMOTE_CLEANUP_AGE that we already hold locally.'''
        now = pd.Timestamp.now().normalize()
        cutoff = now - self._remote_cleanup_age
        oldest = cutoff - self._remote_cleanup_window

        remote_dir = (self._sftp_dir or '.').strip() or '.'
        try:
            remote_names = set(sftp.listdir(remote_dir))
        except IOError as ex:
            self.diag(f'        Could not list remote directory for cleanup: {ex}', 1)
            return

        deleted = 0
        day = cutoff
        while day >= oldest:
            filename = self._remote_filename(day)
            if filename in remote_names:
                local_file = self._local_file_for_date(day)
                if os.path.exists(local_file) and self._has_grib_magic(local_file):
                    try:
                        sftp.remove(self._remote_path(filename))
                        deleted += 1
                        self.diag(f'        Deleted remote {filename} (older than {self._remote_cleanup_age}, held locally).', 1)
                    except IOError as ex:
                        self.diag(f'        Could not delete remote {filename}: {ex}', 1)
            day -= pd.Timedelta(days=1)

        if deleted:
            self.diag(f'        Deleted {deleted} old remote file(s).', 1)
        else:
            self.diag('        No remote files to delete.', 1)

    # ------------------------------------------------------------------ base overrides

    def _download_from_source(self) -> bool:
        self.diag('    Download from source...', 1)

        cutoff = pd.Timestamp.now() - self._publication_memory - self._publication_latency
        pending = (
            self.data_index
            .loc[~self.data_index['local_file_complete'], ['production_datetime', 'local_file']]
            .drop_duplicates()
            .loc[lambda df: df['production_datetime'] >= cutoff]
            .sort_values('production_datetime')
        )

        if pending.empty:
            self.diag('        Nothing to download.', 1)
            return False

        transport = None
        sftp = None
        downloaded = False
        try:
            transport, sftp = self._sftp_connect()
            user, host = self._parse_target()
            self.diag(f'        Connected to {host} as {user}.', 1)

            with DownloadMonitor() as monitor:
                for row in pending.itertuples(index=False):
                    production_datetime = pd.Timestamp(row.production_datetime)
                    local_file = Path(row.local_file)
                    filename = self._remote_filename(production_datetime)
                    remote = self._remote_path(filename)

                    status, detail = self._sftp_download_one(sftp, remote, local_file)
                    if status == 'Downloaded':
                        self.diag('        ' + monitor.mark_success(str(local_file)), 1)
                        downloaded = True
                    elif status == 'Not found':
                        self.diag(f'        Not yet available: {filename}.', 1)
                    else:
                        self.diag(f'        Download failed for {filename}: {detail}', 1)

            try:
                self._cleanup_remote(sftp)
            except Exception as ex:
                self.diag(f'        Remote cleanup skipped: {ex}', 1)
        finally:
            if sftp is not None:
                try:
                    sftp.close()
                except Exception:
                    pass
            if transport is not None:
                try:
                    transport.close()
                except Exception:
                    pass

        if downloaded:
            self._check_existing_data(stored=False, cloud=False)

        return downloaded

    def _convert_units(self, values: np.ndarray) -> np.ndarray:
        values = np.asarray(values, dtype=np.float32).copy()
        if self._variable == 't2m':
            values -= 273.15
        elif self._variable == 'tp':
            values *= 1000.0
            values = np.maximum(values, 0.0)
        else:
            raise Exception(f'Unexpected variable {self._variable} in {self.__class__.__name__}')
        return values

    def read_local(self, local_file: str) -> MeteoRaster:
        self.diag(f'            Reading "{local_file}" ({self.__class__.__name__})', 1)

        leadtimes = pd.to_timedelta(self._leadtimes_local)

        datasets = cfgrib.open_datasets(local_file, backend_kwargs={'indexpath': ''})
        if not datasets:
            raise RuntimeError(f'No cfgrib datasets found in {local_file}.')

        member_slices = []
        for ds in datasets:
            # IPMA GRIB headers are unreliable (variables can be mislabelled), so read whichever
            # single field the file contains rather than filtering by shortName/paramId.
            variable_name = next(iter(ds.data_vars))
            data_array = ds[variable_name]

            if 'time' not in data_array.dims:
                time_value = np.asarray(ds.coords['time'].data).reshape(-1)
                data_array = data_array.expand_dims(time=time_value)
            if 'step' not in data_array.dims:
                step_value = np.asarray(ds.coords['step'].data).reshape(-1)
                data_array = data_array.expand_dims(step=step_value)
            if 'number' not in data_array.dims:
                if 'number' in ds.coords:
                    number_value = np.asarray(ds.coords['number'].data).reshape(-1)
                else:
                    number_value = np.array([0], dtype=int)
                data_array = data_array.expand_dims(number=number_value)

            data_array = data_array.reindex(step=leadtimes)
            member_slices.append(data_array.transpose('time', 'number', 'step', 'latitude', 'longitude'))

        data_array = xr.concat(member_slices, dim='number').sortby('number')

        values = self._convert_units(np.asarray(data_array.data, dtype=np.float32))
        production_datetime = pd.to_datetime(np.asarray(data_array['time'].data))
        latitudes = np.asarray(data_array['latitude'].data)
        longitudes = np.asarray(data_array['longitude'].data)

        payload = {
            'data': values,
            'production_datetime': production_datetime,
            'leadtimes': leadtimes,
            'latitudes': latitudes,
            'longitudes': longitudes,
        }

        raster = MeteoRaster(payload, units=self._units[self._variable], variable=self._variable, verbose=False)
        raster.trim()
        return raster


class IPMA_T2M(IPMA):
    with CaptureNewVariables() as _IPMA_T2M_VARIABLES:
        VARIABLE = 't2m'
        ZONE = 'atlantic'


class IPMA_TP(IPMA):
    with CaptureNewVariables() as _IPMA_TP_VARIABLES:
        VARIABLE = 'tp'
        ZONE = 'atlantic'


class IPMA_ENS(IPMA):
    """IPMA ensemble forecasts (control + 50 perturbed members), equivalent to the ECMWF ENS driver.

    Iberia domain (35-45N, -12--5E @ 0.1 deg), 3-hourly steps to +144 h then 6-hourly to +360 h.
    NOTE: the production remote filename pattern is assumed to be ``Predict_ENS_YYYYMMDD_<name>.grib``
    (only a test sample was available). Update REMOTE_FILE_TEMPLATE once the real pattern is known.
    """

    with CaptureNewVariables() as _IPMA_ENS_VARIABLES:
        PUBLICATION_MEMORY = pd.Timedelta(days=3)

        LEADTIMES = [pd.Timedelta(hours=h) for h in [*range(3, 145, 3), *range(150, 361, 6)]]
        LEADTIMES_LOCAL = [pd.Timedelta(hours=h) for h in [*range(3, 145, 3), *range(150, 361, 6)]]

        ZONE = 'atlantic'

        REMOTE_FILE_TEMPLATE = 'Predict_%Y%m%d_ENS_{name}.grib'

        CLOUD_TEMPLATE = 'IPMA_ENS/{self._variable_upper}/%Y/%m/ipma_ens_{self._variable_lower}_%Y%m%d.grib'
        LOCAL_PATH_TEMPLATE = 'IPMA_ENS/{self._variable_upper}/%Y/%m/ipma_ens_{self._variable_lower}_%Y%m%d.grib'
        STORAGE_PATH_TEMPLATE = 'IPMA_ENS/ipma_ens_{self._variable_lower}_{self._zone}/%Y/%m/tethys_ipma_ens_{self._variable_lower}_%Y%m%d.nct'


class IPMA_ENS_T2M(IPMA_ENS):
    with CaptureNewVariables() as _IPMA_ENS_T2M_VARIABLES:
        VARIABLE = 't2m'
        ZONE = 'atlantic'


class IPMA_ENS_TP(IPMA_ENS):
    with CaptureNewVariables() as _IPMA_ENS_TP_VARIABLES:
        VARIABLE = 'tp'
        ZONE = 'atlantic'


# create_kml_classes(IPMA, {'VARIABLE': ['tp', 't2m']})
# create_kml_classes(IPMA_ENS, {'VARIABLE': ['tp', 't2m']})


if __name__ == '__main__':

    from matplotlib import pyplot as plt
    plt.ion()

    classes = (
        # 'IPMA_TP',
        # 'IPMA_T2M',
        'IPMA_ENS_TP',
        'IPMA_ENS_T2M',
    )

    for cls_name in classes:
        task_cls = globals().get(cls_name)
        task = task_cls(date_from='2026-07-15 00:00:00', download_from_origin=True)
        try:
            task.update()
        except Exception as ex:
            print(f'Error updating {cls_name}: {ex}')
            continue
