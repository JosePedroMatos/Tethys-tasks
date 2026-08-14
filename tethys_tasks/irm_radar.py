'''
IRM (RMI Belgium) radar precipitation archives -> hourly .nct.

Three archives under a single origin root (``origin_folder``, default ``T:\\IRM``):

* ``RADCLIM_TP``   ``radclim/%Y/%m/%d/*.radclim.1h.hdf``               2017-01-01 01:00 -> 2022-12-31 23:00
* ``QPE_TP``       two tars in ``QPE/`` holding 5-min files            2017-01-01 01:00 -> 2022-02-27 15:00
* ``BESTQPE2_TP``  ``bestqpe2/bestqpe2/%Y/%m/%d/*.radqpe2.accum1h.hdf``  2024-04-11 -> ongoing

Design decisions
----------------
* Read in place. The archives already sit on the network drive, so there is no download tier and
  no local copy: the framework's "local" tier IS the origin, and every write path into it is
  disabled (see the guards below). ``origin_folder`` is a normal class variable, so it can be set
  per instance, by kwarg, or through ``IRM_RADAR_FOLDER``.
* One grid for all three products: 700x700 @ 1 km on Belgian Lambert 2008 (EPSG:3812). ODIM only
  stores the projection and the upper-left corner, so the 2-D WGS84 lat/lon arrays MeteoRaster
  keeps natively are computed once with pyproj from ``GRID``/``PROJDEF``. Every file read is
  checked against ``GRID`` so a change of domain fails loudly instead of silently misplacing data.
* Hourly, mm/hr, end-of-interval timestamps. The file timestamp is the END of the accumulation
  (confirmed by ``dataset1/what`` start/end), which matches the ERA5/CERRA convention already used
  in the repo: the value at t covers (t-1h, t]. RADCLIM/BESTQPE2 store ``ACRR`` over one hour, so
  mm == mm/hr and the field is used as-is. QPE stores instantaneous 5-min ``RATE`` in mm/h, so an
  hour is the mean of the twelve samples in (t-1h, t] -- cross-checked against RADCLIM.
* No cropping. These are already small regional grids, so there is no KML/bounding box step.
* Monthly .nct (150-230 MB measured; ~1.5 GB in memory while a chunk is built). ``store()`` is
  overridden because the base version joins one MeteoRaster per source file, which is O(n^2)
  memcpy over the 744 steps of a month (a month takes ~15 s here, ~80 s for QPE).
'''

from __future__ import annotations

import io
import os
import tarfile
import tempfile
import warnings
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

import h5py
import numpy as np
import pandas as pd
import pyproj
from meteoraster import MeteoRaster

from tethys_tasks import BaseTask, CaptureNewVariables, running_in_docker


class IRM_RADAR(BaseTask):
    '''
    Shared behaviour of the IRM ODIM_H5 radar archives: the fixed Lambert grid, single-file
    reading, the read-only-origin guards and the pre-allocated ``store()``.

    Subclasses provide the path templates and, where the source is not one file per stored step
    (``QPE_TP``), override ``_step_sources``/``_fetch``/``_combine``/``_existing_steps``.
    '''

    with CaptureNewVariables() as _IRM_RADAR_VARIABLES:  # name MUST be _<ClassName>_VARIABLES
        # The read-only archive root. Everything else is relative to it.
        ORIGIN_FOLDER = os.getenv('IRM_RADAR_FOLDER', r'T:\IRM')

        # Analysis-like hourly series: production_datetime = end of the accumulation, leadtime 0.
        PRODUCTION_FREQUENCY = pd.Timedelta(hours=1)
        LEADTIMES = pd.timedelta_range('0h', '0h', freq='1h')
        PUBLICATION_LATENCY = pd.Timedelta(days=1)

        # A source file that exists holds valid data; unreadable ones are skipped at read time.
        ASSUME_LOCAL_COMPLETE = True
        # Only needs to span one monthly storage chunk.
        STORAGE_SEARCH_WINDOW = pd.DateOffset(days=35)
        # Threads used for raw byte reads (latency-bound over SMB).
        SOURCE_PARALLEL_TRANSFERS = 8
        # Steps whose sources are fetched concurrently (bounds the buffer memory).
        GROUP_STEPS = 12

        # No cloud/Dropbox tier: .env enables both globally, they must be off here. The cloud
        # folder is only a placeholder so the index can be built without cloud configuration.
        CLOUD_UPLOAD_LOCAL = False
        SYNC_LATEST_STORED = False
        CLOUD_STORAGE_FOLDER = os.getenv('CLOUD_STORAGE_FOLDER') or 'unused'
        CLOUD_TEMPLATE = ''

        VARIABLE = 'tp'
        UNITS = 'mm/hr'
        QUANTITY = 'ACRR'                      # expected dataset1/data1/what/quantity
        DATASET = 'dataset1/data1/data'

        # Grid shared by all three products (checked on every read).
        GRID = dict(ul_x=300000.0, ul_y=1000000.0, xscale=1000.0, yscale=1000.0, xsize=700, ysize=700)
        PROJDEF = ('+proj=lcc +lat_1=49.83333333333334 +lat_2=51.16666666666666 +lat_0=50.797815 '
                   '+lon_0=4.359215833333333 +x_0=649328 +y_0=665262 +ellps=GRS80 '
                   '+towgs84=0,0,0,0,0,0,0 +units=m +no_defs')

        # Closed archive: a step without sources will never arrive, so a chunk is marked complete
        # once every available source has been read (keeps re-runs from reloading every .nct).
        CLOSED_ARCHIVE = True
        # Upper bound of the archive. '' falls back to the BaseTask behaviour (now - latency).
        DATE_TO = ''

        # Recency checks are meaningless for an archive.
        FAIL_IF_OLDER = pd.Timedelta(days=36500)

    # ------------------------------------------------------------------ configuration
    def _set_base_variables(self, cls, kwargs):
        super()._set_base_variables(cls, kwargs)

        # The framework's "local" tier is the origin archive itself.
        self._local_storage_folder = str(self._origin_folder)

    def populate(self, date_from: str = '', date_to: str = '', *args, **kwargs) -> pd.DataFrame:
        # Bound the index by the end of the archive instead of "now".
        if isinstance(date_to, str) and date_to == '' and self._date_to:
            date_to = self._date_to

        return super().populate(date_from, date_to, *args, **kwargs)

    # ------------------------------------------------------------------ grid
    def _latlon(self):
        '''
        2-D WGS84 latitudes/longitudes of the cell centres, computed once per instance.
        Row 0 is the northernmost, so MeteoRaster does not flip the grid.
        '''

        if getattr(self, '_latlon_cache', None) is None:
            grid = self._grid
            x = grid['ul_x'] + (np.arange(int(grid['xsize'])) + 0.5) * grid['xscale']
            y = grid['ul_y'] - (np.arange(int(grid['ysize'])) + 0.5) * grid['yscale']
            xx, yy = np.meshgrid(x, y)
            transformer = pyproj.Transformer.from_crs(pyproj.CRS.from_proj4(self._projdef),
                                                      'EPSG:4326', always_xy=True)
            longitudes, latitudes = transformer.transform(xx, yy)
            self._latlon_cache = (latitudes, longitudes)

        return self._latlon_cache

    # ------------------------------------------------------------------ reading
    @staticmethod
    def _attribute(attrs, key: str) -> str:
        value = attrs[key]
        return value.decode() if isinstance(value, bytes) else str(value)

    def _decode(self, buffer: bytes) -> np.ndarray:
        '''
        Returns the single 700x700 field of an ODIM_H5 buffer, after checking grid and quantity.
        '''

        with h5py.File(io.BytesIO(buffer), 'r') as handle:
            where = handle['dataset1/where'].attrs
            grid = self._grid
            for key, expected in (('UL_x', grid['ul_x']), ('UL_y', grid['ul_y']),
                                  ('xscale', grid['xscale']), ('yscale', grid['yscale']),
                                  ('xsize', grid['xsize']), ('ysize', grid['ysize'])):
                if float(where[key]) != float(expected):
                    raise ValueError(f'Unexpected grid: {key}={where[key]} (expected {expected}).')

            quantity = self._attribute(handle['dataset1/data1/what'].attrs, 'quantity')
            if quantity != self._quantity:
                raise ValueError(f'Unexpected quantity: {quantity} (expected {self._quantity}).')

            return np.asarray(handle[self._dataset][...], dtype='float32')

    def _source_file(self, timestamp: pd.Timestamp) -> Path:
        return Path(self._local_storage_folder) / timestamp.strftime(self._local_path_template)

    def _step_sources(self, timestamp: pd.Timestamp) -> list:
        '''
        Source keys needed to build one stored step. One file per step by default.
        '''

        return [timestamp]

    def _fetch(self, key) -> bytes:
        with open(self._source_file(key), 'rb') as handle:
            return handle.read()

    def _fetch_safe(self, key):
        try:
            return self._fetch(key)
        except Exception as ex:
            self.diag(f'        Source unavailable ({key}): {ex}.', 2)
            return None

    def _combine(self, timestamp: pd.Timestamp, arrays: list):
        '''
        Turns the decoded sources of one step into a single mm/hr field (None when not possible).
        ACRR over one hour is already mm/hr.
        '''

        return arrays[0] if arrays else None

    def _read_step(self, timestamp: pd.Timestamp, buffers: dict = None):
        '''
        Returns the mm/hr field of one stored step, or None when its sources are missing or
        unreadable. ``buffers`` optionally supplies bytes already fetched (see ``store``).
        '''

        keys = self._step_sources(timestamp)
        if not keys:
            return None

        arrays = []
        for key in keys:
            buffer = self._fetch_safe(key) if buffers is None else buffers.get(key)
            if buffer is None:
                continue
            try:
                arrays.append(self._decode(buffer))
            except Exception as ex:
                self.diag(f'        Skipping unreadable source ({key}): {ex}.', 1)

        return self._combine(timestamp, arrays)

    @staticmethod
    def _timestamp_from_path(local_file) -> pd.Timestamp:
        '''
        The step timestamp is the leading %Y%m%d%H%M%S of the file name in every product.
        '''

        return pd.to_datetime(Path(local_file).name[:14], format='%Y%m%d%H%M%S')

    def read_local(self, local_file: str) -> MeteoRaster:
        '''
        Returns a single-step MeteoRaster (mm/hr) for the step the path points at.
        '''

        self.diag(f'            Reading "{local_file}" ({self.__class__.__name__})', 2)

        timestamp = self._timestamp_from_path(local_file)
        array = self._read_step(timestamp)
        if array is None:
            raise Exception(f'No readable source for {timestamp} ({self.__class__.__name__}).')

        latitudes, longitudes = self._latlon()

        return MeteoRaster(dict(data=array[None, None, None, :, :],
                                latitudes=latitudes,
                                longitudes=longitudes,
                                production_datetime=pd.DatetimeIndex([timestamp]),
                                leadtimes=np.array([pd.Timedelta('0h')])),
                           units=self._units, variable=self._variable, verbose=False)

    # ------------------------------------------------------------------ availability
    def _existing_steps(self, timestamps: pd.DatetimeIndex) -> pd.Series:
        '''
        Which steps have a source, by listing each day folder once. The base class rglobs
        Path(local_file).parents[2], which here is a whole year (>100k files) per call.
        '''

        wanted = pd.Series(timestamps.strftime(self._local_path_template.replace('\\', '/')),
                           index=timestamps)

        folders = {}
        for relative in wanted:
            folder, _, name = relative.rpartition('/')
            folders.setdefault(folder, set()).add(name)

        origin = Path(self._local_storage_folder)
        present = set()
        for folder, names in folders.items():
            path = origin / folder
            if not path.is_dir():
                continue
            with os.scandir(path) as entries:
                present.update(f'{folder}/{entry.name}' for entry in entries if entry.name in names)

        return wanted.isin(present)

    def _check_existing_files(self, stored: bool = True, local: bool = True, cloud: bool = True) -> None:
        '''
        Replaces the local branch of the base check: it would recursively glob a whole year of
        source files and build a CompletenessIndex per folder, which creates folders and writes
        completeness.csv inside the read-only origin.
        '''

        super()._check_existing_files(stored=stored, local=False, cloud=cloud)

        if not local:
            return

        self.data_index.loc[:, 'local_file_exists'] = False
        timestamps = pd.DatetimeIndex(self.data_index['production_datetime'].unique())
        if len(timestamps) == 0:
            return

        available = self._existing_steps(timestamps)
        mask = self.data_index['production_datetime'].map(available).fillna(False).astype(bool)
        self.data_index.loc[mask.values, 'local_file_exists'] = True

    def _update_completeness(self, stored: bool = True, local: bool = True) -> None:
        # Never write completeness.csv into the read-only origin.
        super()._update_completeness(stored=stored, local=False)

    # ------------------------------------------------------------------ disabled tiers
    def _check_cloud(self, azure_paths):
        # No cloud tier: never hit Azure.
        return [False] * len(list(azure_paths))

    def _download_from_cloud(self) -> bool:
        self.diag('    Download from Azure skipped (read in place).', 1)
        return False

    def _download_from_source(self) -> bool:
        self.diag('    Download from source skipped (read in place).', 1)
        return False

    def _cleanup_old_files(self) -> None:
        # The base version deletes local files older than MAX_LOCAL_AGE_MONTHS, which here would
        # delete the archive itself.
        self.diag('    Local cleanup skipped (the origin archive is read only).', 1)

    # ------------------------------------------------------------------ storage
    def store(self) -> bool:
        '''
        Fills each monthly .nct in a single pre-allocated pass.

        The base implementation reads one MeteoRaster per source file and joins them, which
        concatenates the growing array once per file (~250 GB of memcpy for a 744-step month).
        Here the chunk is allocated once, pre-filled from whatever is already stored, and only the
        remaining steps are read.
        '''

        stored = False

        self.diag('Storing...', 1)

        self.diag('    Building index and checking completeness...', 2)
        extended_index = self.populate(self.data_index['production_datetime'].min() - self._storage_search_window,
                                       self.data_index['production_datetime'].max() + self._storage_search_window)
        self.data_index = extended_index.loc[extended_index['stored_file'].isin(self.data_index['stored_file'].unique())]
        self._clean_index()
        self._update_index_and_completeness()

        self.diag('    Storing...', 2)
        stored_files = self.data_index.loc[~self.data_index['stored_file_complete'], 'stored_file'].unique()

        # One chunk at a time: each holds ~1.5 GB, and _store_chunk releases it on return.
        with ThreadPoolExecutor(max_workers=self._source_parallel_transfers) as executor:
            for stored_file in stored_files[::-1]:
                stored = self._store_chunk(stored_file, executor) or stored

        self._update_index_and_completeness(local=False, cloud=False)

        return stored

    def _store_chunk(self, stored_file: str, executor: ThreadPoolExecutor) -> bool:
        '''
        Builds one storage chunk in a single pre-allocated pass. Returns True when it was written.
        '''

        latitudes, longitudes = self._latlon()
        index = self.data_index.loc[self.data_index['stored_file'] == stored_file]
        leadtimes = np.array(pd.to_timedelta(index['leadtime'].unique()))
        production_datetimes = pd.DatetimeIndex(np.sort(index['production_datetime'].unique()))
        available = index.groupby('production_datetime')['local_file_exists'].any()
        has_source = np.array([bool(available.get(timestamp, False)) for timestamp in production_datetimes])

        data = np.full((len(production_datetimes), 1, len(leadtimes)) + latitudes.shape,
                       np.nan, dtype='float32')

        previous = self._prefill(stored_file, data, production_datetimes)

        to_read = [(position, timestamp) for position, timestamp in enumerate(production_datetimes)
                   if has_source[position] and not np.isfinite(data[position]).any()]
        if to_read:
            self.diag(f'            Reading {len(to_read)} step(s) for "{stored_file}" ({self.__class__.__name__})', 1)
            self._fill(data, to_read, executor)

        filled = np.isfinite(data).any(axis=(1, 2, 3, 4))
        if not filled.any():
            return False
        if previous is not None and np.array_equal(filled, previous):
            # Nothing new could be added.
            return False

        complete = None
        if self._closed_archive:
            # Steps without sources will never arrive: judge completeness on what exists.
            complete = bool(filled[has_source].all()) if has_source.any() else False

        mr = MeteoRaster(dict(data=data,
                              latitudes=latitudes,
                              longitudes=longitudes,
                              production_datetime=production_datetimes,
                              leadtimes=leadtimes),
                         units=self._units, variable=self._variable, verbose=False)

        self.diag(f'            Saving "{stored_file}" ({self.__class__.__name__})', 1)
        Path(stored_file).parent.mkdir(parents=True, exist_ok=True)
        mr.save(stored_file, complete=complete)
        self.diag(f'                Done ({int(filled.sum())}/{len(filled)} steps).', 1)

        return True

    def _prefill(self, stored_file: str, data: np.ndarray, production_datetimes: pd.DatetimeIndex):
        '''
        Copies an existing .nct into the pre-allocated chunk so its steps are not read again.
        Returns the boolean "step is filled" vector of the existing file (None when there is none).
        '''

        if not Path(stored_file).exists():
            return None

        self.diag(f'            Reading "{stored_file}" ({self.__class__.__name__})', 1)
        mr = self._load_stored_file(stored_file)
        if mr is None:
            return None

        try:
            if mr.data.shape[1:] != data.shape[1:]:
                raise ValueError(f'stored shape {mr.data.shape[1:]} != expected {data.shape[1:]}')
            positions = production_datetimes.get_indexer(pd.DatetimeIndex(mr.production_datetime))
            valid = positions >= 0
            data[positions[valid], ...] = mr.data[valid, ...]
        except Exception as ex:
            print(f'        Stored file ignored, it will be rebuilt: {stored_file} ({ex}).')
            data[...] = np.nan
            return None
        finally:
            del mr

        return np.isfinite(data).any(axis=(1, 2, 3, 4))

    def _fill(self, data: np.ndarray, to_read: list, executor: ThreadPoolExecutor) -> None:
        '''
        Reads the given (position, timestamp) steps into ``data``. Raw bytes are fetched
        concurrently (I/O bound, releases the GIL) in groups of GROUP_STEPS steps, then decoded
        in the calling thread (h5py is not thread safe).
        '''

        group_steps = max(int(self._group_steps), 1)
        for start in range(0, len(to_read), group_steps):
            group = to_read[start:start + group_steps]

            jobs = [(position, key) for position, timestamp in group
                    for key in self._step_sources(timestamp)]
            if not jobs:
                continue

            fetched = list(executor.map(self._fetch_safe, [key for _, key in jobs]))
            buffers = {}
            for (position, key), buffer in zip(jobs, fetched):
                buffers.setdefault(position, {})[key] = buffer

            for position, timestamp in group:
                array = self._read_step(timestamp, buffers=buffers.get(position, {}))
                if array is not None:
                    data[position, 0, 0, :, :] = array


class RADCLIM_TP(IRM_RADAR):
    '''
    RADCLIM: gauge-adjusted (external drift kriging) hourly radar precipitation, 2017-2022.
    '''

    with CaptureNewVariables() as _RADCLIM_TP_VARIABLES:
        LOCAL_PATH_TEMPLATE = 'radclim/%Y/%m/%d/%Y%m%d%H%M%S.radclim.1h.hdf'
        STORAGE_PATH_TEMPLATE = 'RADCLIM/radclim_tp/%Y/tethys_radclim_tp_%Y.%m.01.nct'

        DATE_FROM = '2017-01-01 01:00:00'
        DATE_TO = '2022-12-31 23:00:00'


class BESTQPE2_TP(IRM_RADAR):
    '''
    BESTQPE2: newer hourly radar accumulation (radqpe2), from 2024-04-11 onwards.

    The archive lives in the nested ``bestqpe2/bestqpe2`` tree; the sibling ``bestqpe2/%Y`` tree is
    an incomplete partial copy (a handful of the 24 hourly files per day) and is ignored.
    '''

    with CaptureNewVariables() as _BESTQPE2_TP_VARIABLES:
        LOCAL_PATH_TEMPLATE = 'bestqpe2/bestqpe2/%Y/%m/%d/%Y%m%d%H%M%S.radqpe2.accum1h.hdf'
        STORAGE_PATH_TEMPLATE = 'BESTQPE2/bestqpe2_tp/%Y/tethys_bestqpe2_tp_%Y.%m.01.nct'

        DATE_FROM = '2024-04-11 00:00:00'
        DATE_TO = ''            # still growing
        CLOSED_ARCHIVE = False  # the latest month is still filling in


class QPE_TP(IRM_RADAR):
    '''
    QPE: raw (not gauge-adjusted) 5-min radar rate, resampled to hourly means, 2017 to 2022-02-27.

    The source files are only available inside large tars, which are read in place: the member
    headers of each tar are scanned once (~1 min for 30 GB) into a timestamp -> (offset, size)
    index, cached as parquet, after which any 5-min field is a seek and a ~90 kB read.

    One hour is the mean of the twelve instantaneous mm/h samples in (t-1h, t], which matches the
    end-of-interval convention of the accumulated products. Hours with fewer than MIN_SAMPLES
    samples are dropped: measured over 105 days, 97.7 % of hours have all 12 and >=10 keeps 99.0 %.
    '''

    with CaptureNewVariables() as _QPE_TP_VARIABLES:
        # Virtual paths: the index and the timestamp parsing use them, the bytes come from the tars.
        LOCAL_PATH_TEMPLATE = 'QPE/%Y/%m/%d/%Y%m%d%H%M%S.rad.best.comp.rate.qpe.hdf'
        STORAGE_PATH_TEMPLATE = 'QPE_IRM/qpe_tp/%Y/tethys_qpe_tp_%Y.%m.01.nct'

        DATE_FROM = '2017-01-01 01:00:00'
        DATE_TO = '2022-02-27 15:00:00'

        QUANTITY = 'RATE'
        SAMPLE_FREQUENCY = pd.Timedelta(minutes=5)
        MIN_SAMPLES = 10

        TAR_FOLDER = 'QPE'
        TAR_INDEX_FOLDER = os.getenv('LOCAL_FILE_FOLDER_DOCKER' if running_in_docker() else 'LOCAL_FILE_FOLDER', '')

    # ------------------------------------------------------------------ tar member index
    def _cache_folder(self) -> Path:
        folder = Path(self._tar_index_folder or tempfile.gettempdir()) / 'IRM_QPE_TAR_INDEX'
        folder.mkdir(parents=True, exist_ok=True)
        return folder

    def _tar_members(self, tar: Path) -> pd.DataFrame:
        '''
        (offset, size) of every member of one tar, keyed by timestamp. The cache name carries the
        size and mtime of the archive, so a replaced or extended tar is rescanned.
        '''

        stat = tar.stat()
        cache = self._cache_folder() / f'{tar.stem}__{stat.st_size}__{int(stat.st_mtime)}.parquet'

        if cache.exists():
            try:
                return pd.read_parquet(cache).set_index('stamp')
            except Exception as ex:
                print(f'        Tar index cache unreadable, rebuilding it: {cache} ({ex}).')

        self.diag(f'        Indexing {tar.name} (one-off, about a minute)...', 1)
        rows = []
        with tarfile.open(tar, 'r:') as archive:
            for member in archive:
                if not member.isfile():
                    continue
                name = member.name.rsplit('/', 1)[-1]
                if len(name) < 14 or not name[:14].isdigit():
                    continue
                rows.append((name[:14], member.offset_data, member.size))

        frame = pd.DataFrame(rows, columns=['stamp', 'offset', 'size'])
        frame['stamp'] = pd.to_datetime(frame['stamp'], format='%Y%m%d%H%M%S')
        frame = frame.drop_duplicates('stamp', keep='last')
        frame['tar'] = str(tar)
        self.diag(f'            {len(frame)} members.', 1)

        try:
            frame.to_parquet(cache, index=False)
        except Exception as ex:
            print(f'        Tar index could not be cached: {cache} ({ex}).')

        return frame.set_index('stamp')

    def _tar_index(self) -> pd.DataFrame:
        if getattr(self, '_tar_index_cache', None) is None:
            folder = Path(self._local_storage_folder) / self._tar_folder
            tars = sorted(folder.glob('*.tar'))
            if not tars:
                raise Exception(f'No tar archives in {folder} ({self.__class__.__name__}).')

            index = pd.concat([self._tar_members(tar) for tar in tars]).sort_index()
            # The parquet round-trip and a fresh scan disagree on the string dtype.
            index['tar'] = index['tar'].astype(str)
            self._tar_index_cache = index.loc[~index.index.duplicated(keep='last')]

        return self._tar_index_cache

    # ------------------------------------------------------------------ reading
    def _step_sources(self, timestamp: pd.Timestamp) -> list:
        samples = pd.date_range(timestamp - self._production_frequency + self._sample_frequency,
                                timestamp, freq=self._sample_frequency)
        index = self._tar_index().index

        return [sample for sample in samples if sample in index]

    def _fetch(self, key) -> bytes:
        '''
        One member, by seek and read. The archive is opened per fetch: fetches run in parallel
        threads and a shared handle could not be seeked safely, while the open itself is
        negligible next to the read.
        '''

        row = self._tar_index().loc[key]
        size = int(row['size'])
        with open(row['tar'], 'rb') as handle:
            handle.seek(int(row['offset']))
            buffer = handle.read(size)

        if len(buffer) != size:
            raise OSError(f'Short read ({len(buffer)} of {size} bytes).')

        return buffer

    def _combine(self, timestamp: pd.Timestamp, arrays: list):
        if len(arrays) < int(self._min_samples):
            return None

        with warnings.catch_warnings():
            # All-NaN pixels (outside radar cover) are expected.
            warnings.simplefilter('ignore', category=RuntimeWarning)
            return np.nanmean(np.stack(arrays), axis=0).astype('float32')

    # ------------------------------------------------------------------ availability
    def _existing_steps(self, timestamps: pd.DatetimeIndex) -> pd.Series:
        '''
        An hour exists when enough 5-min members are in the tar index (no filesystem access).
        ceil('1h') maps a sample to the hour it belongs to: exact hour marks stay put, the rest
        move up, which is the (t-1h, t] convention.
        '''

        if getattr(self, '_samples_per_hour', None) is None:
            stamps = self._tar_index().index
            self._samples_per_hour = pd.Series(1, index=stamps.ceil('1h')).groupby(level=0).sum()

        counts = self._samples_per_hour.reindex(timestamps, fill_value=0)

        return counts >= int(self._min_samples)


if __name__ == '__main__':
    import matplotlib.pyplot as plt
    plt.ion()

    kwargs = {}

    # kwargs.update(dict(date_from='2017-01-01', date_to='2022-12-31 23:00:00'))
    # task = RADCLIM_TP(**kwargs)

    kwargs.update(dict(date_from='2017-01-01', date_to='2022-02-27 15:00:00'))
    task = QPE_TP(**kwargs)

    # kwargs.update(dict(date_from='2024-04-11', date_to='2025-10-30 00:00:00'))
    # task = BESTQPE2_TP(**kwargs)
    
    task.update()

    # mr = MeteoRaster.load(task.data_index['stored_file'].unique()[0])
    # mr.plot_mean(coastline=True, borders=True)
    # mr.get_values_from_latlon(50.5, 4.0).plot()
    pass
