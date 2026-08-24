from __future__ import annotations

from pathlib import Path
import bz2
import datetime
import os
import shutil
import tempfile
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from zipfile import ZIP_STORED, ZipFile

import eccodes as ec
import numpy as np
import pandas as pd
from scipy.spatial import cKDTree
from meteoraster import MeteoRaster

from tethys_tasks import BaseTask, CaptureNewVariables, DownloadMonitor, create_kml_classes, running_in_docker


# 00 and 12 UTC runs reach 180 h; the 06 and 18 UTC ones stop at 120 h. LEADTIMES is uniform per
# class, so PRODUCTION_FREQUENCY is 12 h and only the two long runs are collected (as ECMWF_HRES).
_ALL_LEADTIME_HOURS = [*range(0, 79), *range(81, 181, 3)]

# Cell-centre coordinates of the icosahedral mesh (~2.7 MB as published). They are time-invariant
# but too large for the repository, so they are cached in the mounted local folder on first use.
_CONSTANTS_CACHE = Path(
    (os.getenv('LOCAL_FILE_FOLDER_DOCKER') if running_in_docker() else os.getenv('LOCAL_FILE_FOLDER'))
    or Path(__file__).resolve().parent / 'resources'
) / 'ICON_GLOBAL_CONSTANTS'


class ICON_WORLD(BaseTask):
    '''
    DWD ICON global, the 13 km deterministic driver behind opendata.dwd.de/weather/nwp/icon.

    Unlike ICON-EU, which DWD also publishes pre-interpolated to a regular lat-lon grid, ICON
    global is only available on its native icosahedral mesh (2949120 triangle centres,
    gridType 'unstructured_grid'). Two consequences shape this module:

    - cfgrib cannot open the files at all: 'unstructured_grid' is absent from its GRID_TYPE_MAP.
      The messages are therefore decoded with raw eccodes, straight from memory.
    - The cell coordinates are not in the data messages; they come from separate time-invariant
      CLAT/CLON fields, cached by _ensure_constants.

    Remapping to a regular grid is a nearest-neighbour lookup on the unit sphere (see
    _remap_index). meteodatalab.operators.regrid.iconremap, used by icon_ch.py, is not an option
    here: it projects through UTM 32N, which is fine for Switzerland but badly distorted for the
    other zones and unusable globally.

    Only the KML zones are instantiated (see create_kml_classes at the bottom); this base class is
    not usable directly because read_local needs a storage bounding box to build a target grid.

    https://opendatadocs.dwd.de/Data/Forecasts/NWP/
    '''

    with CaptureNewVariables() as _ICON_WORLD_VARIABLES: #It is essential that the format of the variable here is _CLASSnAME_VARIABLES
        # A full run is online ~3.5 h after its reference time (measured: 00 UTC run, step 000 at
        # 02:39, step 180 at 03:28). DWD keeps each slot ~24 h, until the next day overwrites it.
        PUBLICATION_LATENCY = pd.Timedelta(hours=4)
        PUBLICATION_MEMORY = pd.Timedelta(hours=24)
        PRODUCTION_FREQUENCY = pd.Timedelta(hours=12)
        LEADTIMES = [pd.Timedelta(hours=hour) for hour in _ALL_LEADTIME_HOURS]

        SOURCE_PARALLEL_TRANSFERS = 4
        STORAGE_SEARCH_WINDOW = pd.DateOffset(days=10)
        # False, unlike icon_eu.py: read_local_completeness only reads the zip name list, so
        # checking is nearly free, and a short archive (e.g. cached from a run with fewer
        # LEADTIMES) is then completed instead of being silently stored truncated.
        ASSUME_LOCAL_COMPLETE = False

        # 0.125 deg matches the ~13 km (~0.117 deg) native mesh, as DWD's own ICON_GLOBAL2WORLD_0125.
        PIXEL_SIZE = 0.125

        VARIABLE = ''
        ZONE = 'world'
        DOWNLOAD_RETRIES = 2
        DOWNLOAD_RETRY_WAIT = 10

        # Source cells are masked to the target box grown by this margin before the tree is built.
        # Must exceed one cell (~0.117 deg) so that no target pixel misses its true nearest cell.
        REMAP_MARGIN = 0.5

        # Zone-independent local/cloud paths: every zone reads the same downloaded run.
        CLOUD_TEMPLATE = 'ICON_WORLD/{self._variable_upper}/%Y/%m/icon_world_{self._variable_lower}_%Y%m%dT%H.zip'
        LOCAL_PATH_TEMPLATE = 'ICON_WORLD/{self._variable_upper}/%Y/%m/icon_world_{self._variable_lower}_%Y%m%dT%H.zip'
        STORAGE_PATH_TEMPLATE = 'ICON_WORLD/icon_world_{self._variable_lower}_{self._zone}/%Y/%m/tethys_icon_world_{self._variable_lower}_%Y%m%dT%H.nct'

        FAIL_IF_OLDER = pd.Timedelta(hours=24)
        DATE_FROM = (pd.Timestamp.now(datetime.timezone.utc) - pd.Timedelta('2D')).strftime('%Y-%m-%d %H:%M:%S')

        # 'sd' is W_SNOW (snow depth water equivalent, kg m-2 == mm), consistent with icon_ch.py and
        # ECMWF. Note icon_eu.py maps 'sd' to the geometric H_SNOW instead.
        #
        # grib_key is the (discipline, parameterCategory, parameterNumber) triplet. Neither
        # shortName nor paramId can be used to identify the field: both depend on the active
        # ECCODES_DEFINITION_PATH, and the DWD/MeteoSwiss tables turn e.g. 'sd' into 'W_SNOW' and
        # 228141 into 500044. Those tables are only active inside meteodatalab's cosmo_grib_defs
        # context, but the triplet does not move either way, so it is what we match on.
        SOURCE_CONFIG = {
            't2m': {'folder': 't_2m', 'suffix': 'T_2M', 'grib_key': (0, 0, 0), 'units': 'C'},
            'tp': {'folder': 'tot_prec', 'suffix': 'TOT_PREC', 'grib_key': (0, 1, 52), 'units': 'mm/h'},
            'sd': {'folder': 'w_snow', 'suffix': 'W_SNOW', 'grib_key': (0, 1, 60), 'units': 'mm'},
        }

    # Lazily built caches (per instance).
    _cells = None
    _remap = None

    # --- source layout ----------------------------------------------------------------------

    def _member_name(self, production_datetime: pd.Timestamp, leadtime_hour: int) -> str:
        return (
            'icon_global_icosahedral_single-level_'
            f'{production_datetime:%Y%m%d%H}_{leadtime_hour:03d}_{self._source_config[self._variable]["suffix"]}.grib2.bz2'
        )

    def _source_url(self, production_datetime: pd.Timestamp, leadtime_hour: int) -> str:
        folder = self._source_config[self._variable]['folder']
        return (
            'https://opendata.dwd.de/weather/nwp/icon/grib/'
            f'{production_datetime:%H}/{folder}/{self._member_name(production_datetime, leadtime_hour)}'
        )

    @staticmethod
    def _is_unavailable_error(ex: Exception) -> bool:
        message = str(ex).lower()
        return '404' in message or 'not found' in message

    # --- static mesh coordinates ------------------------------------------------------------

    def _ensure_constants(self) -> dict:
        '''
        Fetches CLAT/CLON once into _CONSTANTS_CACHE, keeping them bz2-compressed.

        They are time-invariant but published per run, so the recent run slots are probed until one
        answers (only ~24 h of runs are online at any time).
        '''

        targets = {
            name: _CONSTANTS_CACHE / f'icon_global_icosahedral_time-invariant_{name}.grib2.bz2'
            for name in ('CLAT', 'CLON')
        }
        missing = {name: path for name, path in targets.items() if not path.exists()}
        if not missing:
            return targets

        _CONSTANTS_CACHE.mkdir(parents=True, exist_ok=True)
        now = pd.Timestamp.now(datetime.timezone.utc).tz_localize(None).floor('6h')
        candidates = [now - pd.Timedelta(hours=6 * step) for step in range(5)]

        for name, path in missing.items():
            self.diag(f'        Fetching static mesh coordinates "{name}" ({self.__class__.__name__})', 1)
            errors = []
            for reference in candidates:
                url = (
                    f'https://opendata.dwd.de/weather/nwp/icon/grib/{reference:%H}/{name.lower()}/'
                    f'icon_global_icosahedral_time-invariant_{reference:%Y%m%d%H}_{name}.grib2.bz2'
                )
                fd, temp_name = tempfile.mkstemp(prefix=f'{name}.', suffix='.part', dir=_CONSTANTS_CACHE)
                os.close(fd)
                temp_path = Path(temp_name)
                try:
                    urllib.request.urlretrieve(url, temp_path)
                    if temp_path.read_bytes()[:3] != b'BZh':
                        raise RuntimeError(f'Unexpected file type returned for {url}')
                    shutil.move(str(temp_path), str(path))
                    break
                except Exception as ex:
                    errors.append(f'{url}: {ex}')
                finally:
                    if temp_path.exists():
                        temp_path.unlink()
            else:
                raise RuntimeError(
                    f'Could not fetch the ICON global {name} field from any recent run. Attempts:\n  '
                    + '\n  '.join(errors)
                )

        return targets

    @staticmethod
    def _decode_message(payload: bytes, keys: dict | None = None) -> tuple[dict, np.ndarray]:
        '''
        Decodes one GRIB message from memory. Needed because cfgrib does not support
        'unstructured_grid' (it is absent from cfgrib.dataset.GRID_TYPE_MAP).

        keys maps a GRIB key to the type to read it as. The type has to be given explicitly:
        left to eccodes, 'endStep' comes back as the string '0m' for step 0 and as an int
        afterwards.
        '''

        if payload[:3] == b'BZh':
            payload = bz2.decompress(payload)

        handle = ec.codes_new_from_message(payload)
        try:
            meta = {key: ec.codes_get(handle, key, ktype) for key, ktype in (keys or {}).items()}
            values = ec.codes_get_values(handle)
        finally:
            ec.codes_release(handle)

        return meta, values

    def _cell_coordinates(self) -> tuple[np.ndarray, np.ndarray]:
        if self._cells is None:
            paths = self._ensure_constants()
            _, clat = self._decode_message(paths['CLAT'].read_bytes())
            _, clon = self._decode_message(paths['CLON'].read_bytes())
            if clat.size != clon.size:
                raise RuntimeError(f'Inconsistent ICON global mesh: {clat.size} CLAT vs {clon.size} CLON cells.')
            self._cells = (clat.astype(np.float64), clon.astype(np.float64))

        return self._cells

    # --- remapping --------------------------------------------------------------------------

    def _target_grid(self) -> tuple[np.ndarray, np.ndarray]:
        '''
        Target grid of the zone, derived from the storage bounding box (already snapped to
        PIXEL_SIZE by BaseTask._get_bounding_box). Latitudes descend, as MeteoRaster expects.

        Because the grid is the bounding box, the generic crop in BaseTask.store() is a no-op.
        '''

        box = self.storage_bounding_box
        if box is None:
            raise RuntimeError(
                f'{self.__class__.__name__} has no storage bounding box: ICON global is remapped '
                'onto the grid of a zone, so use one of the classes created by create_kml_classes '
                '(e.g. ICON_WORLD_TP_IBERIA) rather than the base class.'
            )

        pixel = self._pixel_size
        nx = int(np.round((box['east'] - box['west']) / pixel)) + 1
        ny = int(np.round((box['north'] - box['south']) / pixel)) + 1

        return box['north'] - pixel * np.arange(ny), box['west'] + pixel * np.arange(nx)

    @staticmethod
    def _unit_sphere(longitudes, latitudes) -> np.ndarray:
        latitudes = np.deg2rad(np.asarray(latitudes, dtype=np.float64))
        longitudes = np.deg2rad(np.asarray(longitudes, dtype=np.float64))
        cos_latitudes = np.cos(latitudes)
        return np.stack(
            [cos_latitudes * np.cos(longitudes), cos_latitudes * np.sin(longitudes), np.sin(latitudes)],
            axis=-1,
        )

    def _remap_index(self) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
        '''
        Returns (index, latitudes, longitudes), where index holds the nearest source cell of every
        target pixel as a flat position in the 2949120-cell vector.

        Nearest neighbour on the unit sphere, so no projection is involved and neither the
        antimeridian nor the poles need special casing. It is also what DWD's own *_EASY remap
        products do, and at 0.125 deg the target barely differs from the ~0.117 deg source spacing,
        so interpolating would only smooth the fields.
        '''

        if self._remap is not None:
            return self._remap

        latitudes, longitudes = self._target_grid()
        clat, clon = self._cell_coordinates()
        box = self.storage_bounding_box
        margin = self._remap_margin

        west, east = box['west'] - margin, box['east'] + margin
        if west < -180 or east > 180:
            # The grown box wraps the antimeridian; masking would cut off the wrapped side.
            selected = np.arange(clat.size)
        else:
            mask = (
                (clon >= west) & (clon <= east)
                & (clat >= box['south'] - margin) & (clat <= box['north'] + margin)
            )
            selected = np.flatnonzero(mask)
            if selected.size == 0:
                raise RuntimeError(f'No ICON global cell falls within {box} ({self.__class__.__name__}).')

        grid_longitudes, grid_latitudes = np.meshgrid(longitudes, latitudes)
        tree = cKDTree(self._unit_sphere(clon[selected], clat[selected]))
        chords, positions = tree.query(self._unit_sphere(grid_longitudes.ravel(), grid_latitudes.ravel()), k=1)

        # Chord length on the unit sphere -> great-circle distance, as a sanity figure.
        worst = float(np.rad2deg(2 * np.arcsin(np.clip(chords.max(), 0, 2) / 2)) * 111.195)
        self.diag(
            f'            Remap grid {latitudes.size}x{longitudes.size} from {selected.size} cells, '
            f'worst distance {worst:.1f} km ({self.__class__.__name__})',
            2,
        )

        self._remap = (selected[positions], latitudes, longitudes)
        return self._remap

    # --- download ---------------------------------------------------------------------------

    def _download_member(
        self,
        production_datetime: pd.Timestamp,
        tmp_path: Path,
        leadtime_hour: int,
    ) -> tuple[str, Path, int, str | None]:
        target = tmp_path / self._member_name(production_datetime, leadtime_hour)
        url = self._source_url(production_datetime, leadtime_hour)

        for attempt in range(max(int(self._download_retries), 0) + 1):
            try:
                urllib.request.urlretrieve(url, target)
                with target.open('rb') as handle:
                    magic = handle.read(3)
                if magic != b'BZh':
                    return 'Unavailable', target, leadtime_hour, f'Unexpected file type returned for {url}'
                return 'Downloaded', target, leadtime_hour, None
            except urllib.error.HTTPError as ex:
                if target.exists():
                    target.unlink()
                if ex.code == 404:
                    return 'Unavailable', target, leadtime_hour, str(ex)
                if attempt >= max(int(self._download_retries), 0):
                    return 'Failed', target, leadtime_hour, str(ex)
            except Exception as ex:
                if target.exists():
                    target.unlink()
                if attempt >= max(int(self._download_retries), 0):
                    status = 'Unavailable' if self._is_unavailable_error(ex) else 'Failed'
                    return status, target, leadtime_hour, str(ex)

        return 'Failed', target, leadtime_hour, 'Unknown download failure'

    def _production_datetime_for_local_file(self, local_file: str) -> pd.Timestamp:
        rows = self.data_index.loc[self.data_index['local_file'] == local_file, 'production_datetime']
        if rows.empty:
            raise KeyError(f'Local file not present in data index: {local_file}')
        return pd.Timestamp(rows.iloc[0])

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

        leadtime_hours = [int(pd.Timedelta(leadtime) / pd.Timedelta(hours=1)) for leadtime in self._leadtimes]
        workers = max(int(self._source_parallel_transfers), 1)
        self.diag(
            f'        Downloading ICON global with {workers} workers and {max(int(self._download_retries), 0)} retries.',
            1,
        )

        downloaded = False
        unavailable_since = None

        for _, row in pending.iterrows():
            production_datetime = pd.Timestamp(row['production_datetime'])
            if unavailable_since is not None and production_datetime >= unavailable_since:
                self.diag(
                    f'        Skipping {production_datetime:%Y-%m-%d %H:%M} and later production datetimes because files are not available yet.',
                    1,
                )
                break

            local_file = Path(row['local_file'])
            local_file.parent.mkdir(parents=True, exist_ok=True)

            with tempfile.TemporaryDirectory(prefix='icon_world_') as tmp_dir:
                tmp_path = Path(tmp_dir)
                member_files = {}
                failed = False

                with DownloadMonitor() as monitor:
                    executor = ThreadPoolExecutor(max_workers=workers)
                    try:
                        futures = {
                            executor.submit(
                                self._download_member,
                                production_datetime,
                                tmp_path,
                                leadtime_hour,
                            ): leadtime_hour
                            for leadtime_hour in leadtime_hours
                        }

                        for future in as_completed(futures):
                            status, member_path, leadtime_hour, detail = future.result()
                            if status == 'Downloaded':
                                member_files[leadtime_hour] = member_path
                                self.diag('        ' + monitor.mark_success(member_path), 2)
                                continue

                            failed = True
                            if status == 'Unavailable':
                                unavailable_since = production_datetime
                                self.diag(
                                    f'        Files not yet available for {production_datetime:%Y-%m-%d %H:%M} leadtime {leadtime_hour:03d}: {detail}',
                                    1,
                                )
                            else:
                                self.diag(
                                    f'        Download failed for {production_datetime:%Y-%m-%d %H:%M} leadtime {leadtime_hour:03d}: {detail}',
                                    1,
                                )

                            for pending_future in futures:
                                if pending_future is not future:
                                    pending_future.cancel()
                            break
                    finally:
                        executor.shutdown(wait=True, cancel_futures=True)

                if failed:
                    continue

                if len(member_files) != len(leadtime_hours):
                    self.diag(
                        f'        Skipping zip creation for {production_datetime:%Y-%m-%d %H:%M}; only {len(member_files)}/{len(leadtime_hours)} files completed.',
                        1,
                    )
                    continue

                fd, temp_name = tempfile.mkstemp(prefix=local_file.stem + '.', suffix='.part', dir=local_file.parent)
                os.close(fd)
                temp_zip = Path(temp_name)
                try:
                    # ZIP_STORED: the members are already bz2-compressed.
                    with ZipFile(temp_zip, 'w', compression=ZIP_STORED) as archive:
                        for _, member_path in sorted(member_files.items()):
                            archive.write(member_path, arcname=member_path.name)
                    if local_file.exists():
                        local_file.unlink()
                    shutil.move(str(temp_zip), str(local_file))
                    downloaded = True
                finally:
                    if temp_zip.exists():
                        temp_zip.unlink()

        if downloaded:
            self._check_existing_data(stored=False, cloud=False)

        return downloaded

    # --- read -------------------------------------------------------------------------------

    def read_local(self, local_file: str) -> MeteoRaster:
        self.diag(f'            Reading "{local_file}" ({self.__class__.__name__})', 1)

        config = self._source_config[self._variable]
        index, latitudes, longitudes = self._remap_index()
        n_cells = self._cell_coordinates()[0].size

        with ZipFile(local_file, 'r') as archive:
            members = sorted(name for name in archive.namelist() if name.endswith('.grib2.bz2'))
            if not members:
                raise RuntimeError(f'No ICON global files found in {local_file}.')

            production_datetime = None
            leadtimes = []
            values = []

            for member in members:
                meta, cells = self._decode_message(
                    archive.read(member),
                    keys={
                        'discipline': int, 'parameterCategory': int, 'parameterNumber': int,
                        'shortName': str, 'endStep': int, 'dataDate': int, 'dataTime': int,
                        'numberOfDataPoints': int,
                    },
                )

                if meta['numberOfDataPoints'] != n_cells:
                    raise RuntimeError(
                        f'{member} holds {meta["numberOfDataPoints"]} cells but the cached CLAT/CLON '
                        f'describe {n_cells}; the ICON global mesh changed, clear {_CONSTANTS_CACHE}.'
                    )

                grib_key = (meta['discipline'], meta['parameterCategory'], meta['parameterNumber'])
                if grib_key != tuple(config['grib_key']):
                    raise RuntimeError(
                        f'{member} holds {grib_key} ("{meta["shortName"]}") instead of the expected '
                        f'{tuple(config["grib_key"])} for {config["suffix"]} ({self.__class__.__name__}).'
                    )

                if production_datetime is None:
                    production_datetime = pd.Timestamp(str(meta['dataDate'])) + pd.Timedelta(hours=int(meta['dataTime']) // 100)

                leadtimes.append(pd.Timedelta(hours=int(meta['endStep'])))
                values.append(cells[index].reshape(latitudes.size, longitudes.size))

        leadtimes = pd.to_timedelta(leadtimes)
        order = np.argsort(leadtimes.to_numpy())
        leadtimes = leadtimes[order]
        values = np.stack([values[position] for position in order], axis=0).astype(np.float32)

        if self._variable == 't2m':
            values -= 273.15
        elif self._variable == 'sd':
            pass  # W_SNOW is already kg m-2, i.e. mm of water equivalent.
        elif self._variable == 'tp':
            # De-accumulate to a mean rate over the preceding interval, as icon_eu.py does, which
            # keeps LEADTIMES uniform across variables (step 0 carries no interval and is zeroed).
            leadtime_hours = np.asarray([leadtime / pd.Timedelta(hours=1) for leadtime in leadtimes], dtype=float)
            interval_hours = np.diff(leadtime_hours, prepend=0.0)
            values = np.diff(values, axis=0, prepend=values[:1, ...])
            divisor = interval_hours.copy()
            divisor[0] = 1.0
            values = values * 1.0 / divisor.reshape(-1, 1, 1)
            values[0, :, :] = 0.0
            values = np.maximum(values, 0.0)
        else:
            raise Exception(f'Unexpected variable {self._variable} in {self.__class__.__name__}')

        mr_data = {
            'data': values[np.newaxis, np.newaxis, ...],
            'production_datetime': pd.to_datetime([production_datetime]),
            'leadtimes': leadtimes,
            'latitudes': latitudes,
            'longitudes': longitudes,
        }
        return MeteoRaster(
            data=mr_data,
            units=config['units'],
            variable=self._variable,
            verbose=False,
        )

    def read_local_completeness(self, local_file: str) -> pd.Series:
        '''
        Returns the valid steps of a local file, from the zip name list alone.

        The zip is only written once every member has been downloaded, so a member being present is
        enough; there is no need to decode anything.
        '''

        empty = pd.MultiIndex.from_arrays([[], []], names=['production_datetime', 'leadtime'])
        path = Path(local_file)
        if not path.exists():
            return pd.Series(dtype=bool, index=empty)

        production_datetime = self._production_datetime_for_local_file(local_file)
        with ZipFile(path) as archive:
            members = {Path(name).name for name in archive.namelist() if not name.endswith('/')}

        valid = []
        for leadtime in self.data_index.loc[self.data_index['local_file'] == local_file, 'leadtime'].drop_duplicates():
            leadtime_hour = int(pd.Timedelta(leadtime) / pd.Timedelta(hours=1))
            if self._member_name(production_datetime, leadtime_hour) in members:
                valid.append((production_datetime, pd.Timedelta(leadtime)))

        if not valid:
            return pd.Series(dtype=bool, index=empty)

        return pd.Series(True, index=pd.MultiIndex.from_tuples(valid, names=['production_datetime', 'leadtime']))


create_kml_classes(ICON_WORLD, {'VARIABLE': ['tp', 't2m', 'sd']})


if __name__ == '__main__':
    import matplotlib.pyplot as plt
    plt.ion()

    # DWD retention is ~24 h, so date_from has to be recent.
    date_from = (pd.Timestamp.now(datetime.timezone.utc).tz_localize(None) - pd.Timedelta('30h')).strftime('%Y-%m-%d %H:%M:%S')

    for variable in ['tp', 'sd', 't2m']:
        name = f'ICON_WORLD_{variable.upper()}_IBERIA'
        task_cls = globals().get(name)
        if task_cls is None:
            raise RuntimeError(f'{name} was not created at runtime.')

        task = task_cls(date_from=date_from, download_from_origin=True)
        try:
            task.update()
            print(task.acquisition_status(refresh=True))
        except Exception as ex:
            print(f'Error updating {name}: {ex}')

    # files = task.data_index['stored_file'].unique()
    # mr = MeteoRaster.load(files[-2])
    # mr.plot_mean(coastline=True, borders=True)
    # mr.get_values_from_latlon(40, -8).T.plot()

    pass
