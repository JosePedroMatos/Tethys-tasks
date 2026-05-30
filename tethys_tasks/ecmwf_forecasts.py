from __future__ import annotations

from pathlib import Path
import os
import io
import shutil
import tempfile
import datetime
import contextlib
from collections import deque
from concurrent.futures import Future, ThreadPoolExecutor, TimeoutError as FutureTimeoutError
from zipfile import ZIP_DEFLATED, ZipFile

import numpy as np
import pandas as pd
import xarray as xr
import cfgrib
import ecmwf.opendata.client as ecmwf_client
from ecmwf.opendata import Client
from meteoraster import MeteoRaster

from tethys_tasks import BaseTask, CaptureNewVariables, DownloadMonitor, create_kml_classes


class ECMWF_ENS(BaseTask):
    """ECMWF IFS ensemble single-level tasks backed by the Open Data mirrors."""

    with CaptureNewVariables() as _ECMWF_ENS_VARIABLES:
        PUBLICATION_LATENCY = pd.Timedelta(hours=7)
        PUBLICATION_MEMORY = pd.Timedelta(days=2)
        PRODUCTION_FREQUENCY = pd.Timedelta(hours=24)
        LEADTIMES_LOCAL = [pd.Timedelta(hours=hour) for hour in [*range(0, 145, 3), *range(150, 361, 6)]]   # Set of leadtimes actually present in the downloaded files; may be a superset of the final LEADTIMES used in the output rasters after postprocessing.
        LEADTIMES = [pd.Timedelta(hours=hour) for hour in [*range(0, 145, 3), *range(150, 361, 6)]]         # Actual leadtimes saved in the local files. Modified by _derived_leadtimes if needed based on the variable's characteristics.
        DOWNLOAD_CHUNK_REFS = [*range(0, 361-6, 6)]

        CLOUD_UPLOAD_LOCAL = False
        SYNC_LATEST_STORED = False

        SOURCE_PARALLEL_TRANSFERS = 6
        STORAGE_SEARCH_WINDOW = pd.DateOffset(days=14)
        ASSUME_LOCAL_COMPLETE = True

        PIXEL_SIZE = 0.25
        VARIABLE = ''
        ZONE = 'world'
        STREAM = 'enfo'
        REQUEST_TYPES = ['pf']
        DOWNLOAD_RETRIES = 3
        DOWNLOAD_RETRY_WAIT = 60
        DOWNLOAD_TIMEOUT = 150  # seconds per chunk; exceeded chunks are treated as Failed

        CLOUD_TEMPLATE = 'ECMWF_ENS_TEST/{self._variable_upper}/%Y/%m/ecmwf_ens_{self._variable_lower}_%Y%m%dT%H.zip'
        LOCAL_PATH_TEMPLATE = 'ECMWF_ENS_TEST/{self._variable_upper}/%Y/%m/ecmwf_ens_{self._variable_lower}_%Y%m%dT%H.zip'
        STORAGE_PATH_TEMPLATE = 'ECMWF_ENS_TEST/ecmwf_ens_{self._variable_lower}_{self._zone}/%Y/%m/tethys_ecmwf_ens_{self._variable_lower}_%Y%m%d.nct'

        FAIL_IF_OLDER = pd.Timedelta(hours=24)
        DATE_FROM = (pd.Timestamp.now(datetime.timezone.utc) - pd.Timedelta('2D')).strftime('%Y-%m-%d %H:%M:%S')

        PARAMS = {
            't2m': '2t',
            'tp': 'tp',
            'sd': 'sd',
        }

        BACKEND_KWARGS = {
            't2m': {'filter_by_keys': {'typeOfLevel': 'heightAboveGround', 'level': 2}},
            'tp': {'filter_by_keys': {'typeOfLevel': 'surface'}},
            'sd': {'filter_by_keys': {'typeOfLevel': 'surface'}},
        }

        UNITS = {
            't2m': 'C',
            'tp': 'mm',
            'sd': 'mm',
        }

    def _set_base_variables(self, cls, kwargs):
        super()._set_base_variables(cls, kwargs)

        self._leadtimes_local = [pd.Timedelta(lt) for lt in pd.to_timedelta(getattr(self, '_leadtimes_local', self._leadtimes))]
        self._leadtimes = self._derived_leadtimes(self._leadtimes_local)

    def _derived_leadtimes(self, leadtimes_local: list[pd.Timedelta]) -> list[pd.Timedelta]:
        '''
        Derive the leadtimes present in the local files based on the variable's characteristics.
        '''
        if self._variable == 'tp':
            return list(leadtimes_local[:-1])
        return list(leadtimes_local)

    def _forecast_hours(self) -> list[int]:
        '''
        Returns the forecast leadtimes actually present in the downloaded files
        '''
        return [int(pd.Timedelta(hour) / pd.Timedelta(hours=1)) for hour in self._leadtimes_local]

    def _chunk_leadtime_hours(self, ref_hour: int) -> list[int]:
        '''
        Returns the forecast leadtime hours corresponding to the given chunk ref hour.
        '''
        forecast_hours = self._forecast_hours()
        if ref_hour not in self._download_chunk_refs:
            raise ValueError(f'{ref_hour} is not a configured chunk ref for {self.__class__.__name__}.')

        ref_index = self._download_chunk_refs.index(ref_hour)
        next_ref = self._download_chunk_refs[ref_index + 1] if ref_index + 1 < len(self._download_chunk_refs) else forecast_hours[-1] + 1
        return [hour for hour in forecast_hours if ref_hour <= hour < next_ref]

    def _retrieve_to_target(
        self,
        client: Client,
        production_datetime: pd.Timestamp,
        target: Path,
        leadtime_hours: list[int],
    ) -> None:
        result = client._get_urls(
            use_index=True,
            target=str(target),
            date=production_datetime.strftime('%Y-%m-%d'),
            time=production_datetime.hour,
            stream=self._stream,
            type=self._request_types,
            step=leadtime_hours,
            param=self._params[self._variable],
        )

        if client.use_sas_token:
            result.urls = client._apply_sas_to_urls(result.urls)

        with contextlib.redirect_stderr(io.StringIO()):
            ecmwf_client.download(
                result.urls,
                target=result.target,
                verify=client.verify,
                session=client.session,
                maximum_retries=max(int(self._download_retries), 0),
                retry_after=max(float(self._download_retry_wait), 0.0),
            )

    @staticmethod
    def _is_unavailable_error(ex: Exception) -> bool:
        message = str(ex).lower()
        return '404' in message or 'not found' in message

    def _download_chunk(
        self,
        production_datetime: pd.Timestamp,
        tmp_path: Path,
        ref_hour: int,
        leadtime_hours: list[int],
    ) -> tuple[str, Path, int, str | None]:
        chunk_path = tmp_path / f'{self._variable_lower}_{production_datetime:%Y%m%dT%H}_{ref_hour:03d}.grib2' #self._chunk_member_name(production_datetime, ref_hour)

        try:
            client = Client(source='azure', model='ifs', resol='0p25')
            self._retrieve_to_target(client, production_datetime, chunk_path, leadtime_hours)
            with chunk_path.open('rb') as handle:
                magic = handle.read(4)
            if magic != b'GRIB':
                return (
                    'Unavailable',
                    chunk_path,
                    ref_hour,
                    f'Unexpected file type returned for {production_datetime:%Y-%m-%d %H:%M} chunk {ref_hour:03d}.',
                )
            return 'Downloaded', chunk_path, ref_hour, None
        except Exception as ex:
            if chunk_path.exists():
                chunk_path.unlink()
            status = 'Unavailable' if self._is_unavailable_error(ex) else 'Failed'
            return status, chunk_path, ref_hour, str(ex)

    def _production_datetime_for_local_file(self, local_file: str) -> pd.Timestamp:
        rows = self.data_index.loc[self.data_index['local_file'] == local_file, 'production_datetime']
        if rows.empty:
            raise KeyError(f'Local file not present in data index: {local_file}')
        return pd.Timestamp(rows.iloc[0])

    def _assemble_production_zip(
        self,
        local_file: Path,
        chunk_files: dict[int, Path],
    ) -> None:
        fd, temp_name = tempfile.mkstemp(
            prefix=local_file.stem + '.',
            suffix='.part',
            dir=local_file.parent,
        )
        os.close(fd)
        temp_zip = Path(temp_name)
        try:
            with ZipFile(temp_zip, 'w', compression=ZIP_DEFLATED) as archive:
                for _, chunk_file in sorted(chunk_files.items()):
                    archive.write(chunk_file, arcname=chunk_file.name)
            if local_file.exists():
                local_file.unlink()
            shutil.move(str(temp_zip), str(local_file))
        finally:
            if temp_zip.exists():
                temp_zip.unlink()

    def _download_from_source(self) -> bool:
        self.diag('    Download from source...', 1)

        cutoff = pd.Timestamp.now(datetime.timezone.utc).tz_localize(None) - self._publication_memory - self._publication_latency
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

        workers = max(int(self._source_parallel_transfers), 1)
        self.diag(
            f'        Using source "azure" with {workers} workers and {max(int(self._download_retries), 0)} retries.',
            1,
        )

        production_jobs = sorted(
            (
                (pd.Timestamp(row.production_datetime), Path(row.local_file))
                for row in pending.itertuples(index=False)
            ),
            key=lambda item: (item[0], str(item[1])),
        )

        dt_chunk_count = len(self._download_chunk_refs)
        dt_to_local_file = {dt: lf for dt, lf in production_jobs}
        dt_chunks_received: dict[pd.Timestamp, int] = {dt: 0 for dt, _ in production_jobs}
        dt_chunk_files: dict[pd.Timestamp, dict[int, Path]] = {dt: {} for dt, _ in production_jobs}
        dt_failed: set[pd.Timestamp] = set()
        stop_from_dt: pd.Timestamp | None = None
        chunk_attempts: dict[tuple[pd.Timestamp, int], int] = {}  # (dt, ref_hour) -> attempts so far

        # Create a temp dir per production datetime; cleaned up in the finally block.
        temp_dirs: dict[pd.Timestamp, tempfile.TemporaryDirectory] = {}
        for dt, local_file in production_jobs:
            local_file.parent.mkdir(parents=True, exist_ok=True)
            temp_dirs[dt] = tempfile.TemporaryDirectory(prefix='ecmwf_ens_')

        # Flat list of chunk jobs ordered by (production_datetime, ref_hour).
        all_chunk_jobs = [
            (dt, ref_hour)
            for dt, _ in production_jobs
            for ref_hour in self._download_chunk_refs
        ]
        jobs_iter = iter(all_chunk_jobs)

        def submit_chunk(executor: ThreadPoolExecutor, dt: pd.Timestamp, ref_hour: int) -> Future:
            chunk_attempts[(dt, ref_hour)] = chunk_attempts.get((dt, ref_hour), 0) + 1
            return executor.submit(
                self._download_chunk,
                dt,
                Path(temp_dirs[dt].name),
                ref_hour,
                self._chunk_leadtime_hours(ref_hour),
            )

        def next_chunk_job(executor: ThreadPoolExecutor) -> tuple[pd.Timestamp, int, Future] | None:
            for dt, ref_hour in jobs_iter:
                if stop_from_dt is not None and dt >= stop_from_dt:
                    continue  # drain without submitting
                return dt, ref_hour, submit_chunk(executor, dt, ref_hour)
            return None

        downloaded = False
        executor = ThreadPoolExecutor(max_workers=workers)
        try:
            with DownloadMonitor() as monitor:
                in_flight: deque[tuple[pd.Timestamp, int, Future]] = deque()
                for _ in range(workers):
                    job = next_chunk_job(executor)
                    if job is None:
                        break
                    in_flight.append(job)

                while in_flight:
                    dt, ref_hour, future = in_flight.popleft()
                    try:
                        status, chunk_path, _, detail = future.result(timeout=self._download_timeout)
                    except FutureTimeoutError:
                        status, chunk_path, detail = 'Failed', None, f'timed out after {self._download_timeout}s'

                    if status == 'Failed' and chunk_attempts.get((dt, ref_hour), 0) <= self._download_retries:
                        self.diag(
                            f'        Retrying {dt:%Y-%m-%d %H:%M} chunk {ref_hour:03d} (attempt {chunk_attempts[(dt, ref_hour)]}/{self._download_retries}): {detail}',
                            1,
                        )
                        in_flight.append((dt, ref_hour, submit_chunk(executor, dt, ref_hour)))
                        job = next_chunk_job(executor)
                        if job is not None:
                            in_flight.append(job)
                        continue

                    dt_chunks_received[dt] += 1

                    if status == 'Downloaded':
                        dt_chunk_files[dt][ref_hour] = chunk_path
                        self.diag('        ' + monitor.mark_success(chunk_path), 1)
                    elif status == 'Unavailable':
                        dt_failed.add(dt)
                        self.diag(
                            f'        Files not yet available for {dt:%Y-%m-%d %H:%M} chunk {ref_hour:03d}: {detail}',
                            1,
                        )
                        if stop_from_dt is None:
                            stop_from_dt = dt
                            self.diag(
                                f'        Stopping scheduling from {dt:%Y-%m-%d %H:%M}; chunks for this and later production datetimes will not be scheduled.',
                                1,
                            )
                            remaining: deque[tuple[pd.Timestamp, int, Future]] = deque()
                            for idt, iref, ifuture in in_flight:
                                if idt >= stop_from_dt:
                                    ifuture.cancel()
                                    dt_failed.add(idt)
                                else:
                                    remaining.append((idt, iref, ifuture))
                            in_flight = remaining
                    else:
                        dt_failed.add(dt)
                        self.diag(
                            f'        Download failed for {dt:%Y-%m-%d %H:%M} chunk {ref_hour:03d}: {detail}',
                            1,
                        )

                    # When all chunks for a production datetime have been accounted for, assemble or skip.
                    if dt_chunks_received[dt] == dt_chunk_count:
                        if dt not in dt_failed:
                            try:
                                self._assemble_production_zip(dt_to_local_file[dt], dt_chunk_files[dt])
                                downloaded = True
                            except Exception as ex:
                                self.diag(f'        Failed to assemble zip for {dt:%Y-%m-%d %H:%M}: {ex}', 1)
                        else:
                            self.diag(
                                f'        Skipping zip creation for {dt:%Y-%m-%d %H:%M}; only {len(dt_chunk_files[dt])}/{dt_chunk_count} chunks completed.',
                                1,
                            )

                    job = next_chunk_job(executor)
                    if job is not None:
                        in_flight.append(job)
        finally:
            executor.shutdown(wait=False, cancel_futures=True)
            for tmp_dir in temp_dirs.values():
                tmp_dir.cleanup()

        if downloaded:
            self._check_existing_data(stored=False, cloud=False)

        return downloaded

    def _read_chunk_local(self, local_file: str) -> MeteoRaster:
        backend_kwargs = dict(self._backend_kwargs[self._variable])
        backend_kwargs['indexpath'] = ''
        leadtimes_local = pd.to_timedelta(self._leadtimes_local)

        datasets = cfgrib.open_datasets(local_file, **backend_kwargs)
        if not datasets:
            raise RuntimeError(f'No cfgrib datasets found in {local_file}.')

        ensemble_slices = []
        for ds in datasets:
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

            data_array = data_array.reindex(step=leadtimes_local)

            ensemble_slices.append(data_array.transpose('time', 'number', 'step', 'latitude', 'longitude'))

        data_array = xr.concat(ensemble_slices, dim='number').sortby('number')

        values = np.asarray(data_array.data, dtype=np.float32)
        production_datetime = pd.to_datetime(np.asarray(data_array['time'].data))
        latitudes = np.asarray(data_array['latitude'].data)
        longitudes = np.asarray(data_array['longitude'].data)

        payload = {
            'data': values,
            'production_datetime': production_datetime,
            'leadtimes': leadtimes_local,
            'latitudes': latitudes,
            'longitudes': longitudes,
        }

        return MeteoRaster(payload, variable=self._variable, verbose=False)

    def _postprocess_joined_raster(self, raster: MeteoRaster) -> MeteoRaster:
        values = np.asarray(raster.data, dtype=np.float32).copy()
        leadtimes_local = pd.to_timedelta(raster.leadtimes)
        leadtimes = pd.to_timedelta(self._leadtimes)

        if self._variable == 't2m':
            values -= 273.15
            units = self._units[self._variable]
        elif self._variable == 'sd':
            values *= 1000.0
            units = self._units[self._variable]
        elif self._variable == 'tp':
            values = np.diff(values, axis=2, prepend=np.zeros_like(values[:, :, :1, :, :]))
            values *= 1000.0
            values = np.maximum(values, 0.0)
            units = self._units[self._variable]
        else:
            raise Exception(f'Unexpected variable {self._variable} in {self.__class__.__name__}')

        local_index = pd.Index(leadtimes_local)
        output_index = pd.Index(leadtimes)
        output_positions = local_index.get_indexer(output_index)
        if (output_positions < 0).any():
            raise KeyError(f'Could not align output leadtimes for {self.__class__.__name__}.')
        values = values[:, :, output_positions, :, :]

        payload = {
            'data': values,
            'production_datetime': pd.to_datetime(raster.production_datetime),
            'leadtimes': leadtimes,
            'latitudes': np.asarray(raster.latitudes),
            'longitudes': np.asarray(raster.longitudes),
        }

        processed = MeteoRaster(payload, units=units, variable=self._variable, verbose=False)
        processed.trim()
        return processed

    def read_local(self, local_file: str) -> MeteoRaster:
        self.diag(f'            Reading "{local_file}" ({self.__class__.__name__})', 1)

        with tempfile.TemporaryDirectory(prefix='ecmwf_ens_read_') as tmp_dir:
            tmp_path = Path(tmp_dir)
            with ZipFile(local_file, 'r') as archive:
                archive.extractall(tmp_path)

            chunk_files = sorted(tmp_path.glob('*.grib2'))
            if not chunk_files:
                raise RuntimeError(f'No chunk GRIB files found in {local_file}.')

            raster = None
            for chunk_file in chunk_files:
                chunk_raster = self._read_chunk_local(str(chunk_file))
                if raster is None:
                    raster = chunk_raster
                else:
                    raster.join(chunk_raster, strickt=True)
                    # raster.get_values_from_latlon(40, -8).T
            return self._postprocess_joined_raster(raster)

class ECMWF_ENS_T2M_WORLD(ECMWF_ENS):
    with CaptureNewVariables() as _ECMWF_ENS_T2M_WORLD_VARIABLES:
        VARIABLE = 't2m'
        ZONE = 'world'


class ECMWF_ENS_TP_WORLD(ECMWF_ENS):
    with CaptureNewVariables() as _ECMWF_ENS_TP_WORLD_VARIABLES:
        VARIABLE = 'tp'
        ZONE = 'world'


class ECMWF_ENS_SD_WORLD(ECMWF_ENS):
    with CaptureNewVariables() as _ECMWF_ENS_SD_WORLD_VARIABLES:
        VARIABLE = 'sd'
        ZONE = 'world'


class ECMWF_HRES(ECMWF_ENS):
    """ECMWF IFS high-resolution deterministic tasks backed by the Open Data mirrors.

    Downloads the 00z and 12z runs (full 0–360 h range) using stream=oper, type=fc.
    The 06z/18z short-cutoff runs (0–144 h only) are not covered here; subclass with
    PRODUCTION_FREQUENCY=6h and LEADTIMES_LOCAL limited to 0–144 h if needed.
    """

    with CaptureNewVariables() as _ECMWF_HRES_VARIABLES:
        PRODUCTION_FREQUENCY = pd.Timedelta(hours=12)
        DOWNLOAD_CHUNK_REFS = [0, 150]

        STREAM = 'oper'
        REQUEST_TYPES = ['fc']

        CLOUD_TEMPLATE = 'ECMWF_HRES_TEST/{self._variable_upper}/%Y/%m/ecmwf_hres_{self._variable_lower}_%Y%m%dT%H.zip'
        LOCAL_PATH_TEMPLATE = 'ECMWF_HRES_TEST/{self._variable_upper}/%Y/%m/ecmwf_hres_{self._variable_lower}_%Y%m%dT%H.zip'
        STORAGE_PATH_TEMPLATE = 'ECMWF_HRES_TEST/ecmwf_hres_{self._variable_lower}_{self._zone}/%Y/%m/tethys_ecmwf_hres_{self._variable_lower}_%Y%m%d.nct'

class ECMWF_HRES_T2M_WORLD(ECMWF_HRES):
    with CaptureNewVariables() as _ECMWF_HRES_T2M_WORLD_VARIABLES:
        VARIABLE = 't2m'
        ZONE = 'world'


class ECMWF_HRES_TP_WORLD(ECMWF_HRES):
    with CaptureNewVariables() as _ECMWF_HRES_TP_WORLD_VARIABLES:
        VARIABLE = 'tp'
        ZONE = 'world'


class ECMWF_HRES_SD_WORLD(ECMWF_HRES):
    with CaptureNewVariables() as _ECMWF_HRES_SD_WORLD_VARIABLES:
        VARIABLE = 'sd'
        ZONE = 'world'


create_kml_classes(ECMWF_ENS, {'VARIABLE': ['tp', 't2m', 'sd']})
create_kml_classes(ECMWF_HRES, {'VARIABLE': ['tp', 't2m', 'sd']})


if __name__ == '__main__':
    import matplotlib.pyplot as plt
    plt.ion()

    variables = ['t2m', 'tp', 'sd']
    # variables = ['tp']

    for v in variables:

        cls = f'ECMWF_ENS_{v.upper()}_IBERIA'
        # cls = f'ECMWF_HRES_{v.upper()}_WORLD'

        task_cls = globals().get(cls)
        if task_cls is None:
            raise RuntimeError(f'Class {cls} was not created at runtime.')

        task = task_cls(
            date_from='2026-04-14 00:00:00',
            download_from_source=True,
        )

        try:
            task.retrieve()
            # task.update()

            # files = task.data_index['stored_file'].unique()

            # coords = (29.3, 88.7)
            # coords = (30, 88.7)
            # coords = (62, -43.5)
            # mr = MeteoRaster.load(files[-2])
            # mr = mr.get_cropped(from_lat=20, to_lat=60, from_lon=40, to_lon=100)
            # mr.plot_mean(coastline=True, borders=True)
            # mr.get_values_from_latlon(*coords).T.plot()

            # data = mr.get_values_from_latlon(*coords).T.droplevel('ensemble_member')
            # ax = plt.figure().add_subplot(111)
            # for i0 in range(data.shape[1]):
            #     tmp = data.iloc[:, [i0]]
            #     tmp.index += tmp.columns[0]
            #     if v=='tp':
            #         tmp = tmp.cumsum()  
            #     tmp.plot(ax=ax, label=str(tmp.columns[0]))

            pass
        except Exception as ex:
            print(f'Error updating {cls}: {ex}')
            continue
    pass