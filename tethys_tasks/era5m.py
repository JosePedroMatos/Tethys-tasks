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

# MeteoRaster.load() accepts dtype from v3.0 onwards; the guard keeps older wheels
# working. A yearly world cube (12x1801x3600) is 311 MB at float32 against 622 MB at
# float64, so this halves what a stored file is held at.
_LOAD_FLOAT32 = {"dtype": "float32"} if "dtype" in inspect.signature(MeteoRaster.load).parameters else {}

# ECMWF GRIB1 table-128 parameter numbers, used by import_from_archive() to confirm an
# archive holds the variable the task expects before anything is written.
_GRIB1_PARAMETER = dict(t2m=167, tp=228, sd=141)

# MARS experiment version of the final reanalysis. '0005' is ERA5T, the preliminary
# release that gets replaced ~3 months later, so it is never imported.
_FINAL_EXPVER = '0001'


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
        # Storage (and hence Dropbox, which mirrors it) is flat: one file per year, so a
        # year folder would hold exactly one .nct. Every folder level here is static once
        # {self._variable} is resolved, which keeps _stored_retention_scope's prune root
        # per-variable instead of widening it to the shared root.
        STORAGE_PATH_TEMPLATE = 'ERA5Land/Monthly/era5m_{self._variable}_world/tethys_era5m_{self._variable}_%Y.nct'

        # ERA5-Land monthly means carry wrong ACCUMULATED variables from Sep 2022 to Feb
        # 2024 (the Bologna data-centre migration): tp comes out at ~half its correct value.
        # The fault is not detectable per message -- an affected 31-day month has
        # byte-identical grib headers to a good one -- so the window has to be hardcoded.
        # These months are never acquired, and reading one raises instead of letting it
        # reach a stored file. ECMWF's workaround is the by-hour-of-day product, which this
        # driver does not request.
        # https://forum.ecmwf.int/t/data-conflict-era5-land-monthly-averaged-data-from-1950-to-present/1309
        FAULTY_PERIOD_FROM = '2022-09-01'
        FAULTY_PERIOD_TO = '2024-02-29'
        # Only accumulated variables are affected; ssrd/ssr would belong here if ever added.
        FAULTY_PERIOD_VARIABLES = ('tp',)

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

    def _in_faulty_period(self, production_datetime) -> bool:
        '''True for months whose data is known bad (see FAULTY_PERIOD_FROM).'''

        if self._variable not in self._faulty_period_variables:
            return False
        return (pd.Timestamp(self._faulty_period_from)
                <= pd.Timestamp(production_datetime)
                <= pd.Timestamp(self._faulty_period_to))

    def _reject_faulty_period(self, local_file) -> None:
        '''
        Raises if a local file falls in the faulty window. The production datetime comes
        from the index when the file is in it, else from the grib itself, so a manually
        placed or restored file is caught too.
        '''

        rows = self.data_index.loc[self.data_index['local_file'] == local_file, 'production_datetime']
        if len(rows) > 0:
            production_datetime = pd.Timestamp(rows.iloc[0])
        else:
            try:
                production_datetime = self._grib_production_datetime(local_file)
            except OSError:
                # Unreadable: leave it to the caller's discard-and-unlink handling rather
                # than turning a corrupt file into a faulty-window error.
                return
        if self._in_faulty_period(production_datetime):
            raise Exception(
                f'"{Path(local_file).name}" is {production_datetime.strftime("%Y-%m")}, inside the known-bad '
                f'ERA5-Land window {self._faulty_period_from}..{self._faulty_period_to} for '
                f'"{self._variable}". This file must not be stored -- delete it '
                f'({self.__class__.__name__}).')

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

        info, faulty = [], []
        for local_path in files_to_download:
            rows = self.data_index.loc[self.data_index['local_file'] == local_path]
            date = pd.Timestamp(rows['production_datetime'].iloc[0]).replace(day=1, hour=0)

            # Never request a month that is not published yet.
            if date > self.last_production_datetime:
                continue

            if self._in_faulty_period(date):
                faulty.append(date)
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

        if faulty:
            # Loud, but not fatal: these months stay permanently absent, so raising here
            # would make every full-history run fail. A request that is ENTIRELY faulty is
            # a different matter -- it can only ever produce nothing.
            self.diag(f'        Skipping {len(faulty)} month(s) in the known-bad '
                      f'{self._faulty_period_from}..{self._faulty_period_to} window.', 1)
            if not info:
                raise Exception(
                    f'Every requested month falls in the known-bad ERA5-Land window '
                    f'{self._faulty_period_from}..{self._faulty_period_to} for "{self._variable}" '
                    f'({self.__class__.__name__}).')

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

    # ------------------------------------------------------- archive import
    @staticmethod
    def _grib1_messages(archive_file):
        '''
        Yields (offset, length, parameter, production_datetime, expver) for every GRIB1
        message in a concatenated archive, reading headers only.

        Messages are self-delimiting, so this never hands the whole archive to eccodes --
        which is what keeps a multi-GB archive readable on Windows, where eccodes gives up
        around 4 GB. Offsets are plain file positions, so archive size is irrelevant.

        GRIB1 layout used here: section 0 is 'GRIB' + 3-byte total length + edition. The
        PDS then carries the parameter (octet 9), the reference date (octets 13-16 with the
        century at octet 25) and, in the ECMWF local definition, MARS expver (octets 46-49).
        '''

        archive_file = Path(archive_file)
        size = archive_file.stat().st_size
        offset = 0
        with open(archive_file, 'rb') as f:
            while offset < size:
                f.seek(offset)
                section0 = f.read(8)
                if len(section0) < 8 or section0[:4] != b'GRIB':
                    raise OSError(f'No GRIB message at offset {offset} of "{archive_file.name}".')
                if section0[7] != 1:
                    raise OSError(f'Only GRIB edition 1 is supported, found edition {section0[7]} '
                                  f'at offset {offset} of "{archive_file.name}".')
                length = int.from_bytes(section0[4:7], 'big')

                pds = f.read(49)
                if len(pds) < 49:
                    raise OSError(f'Truncated PDS at offset {offset} of "{archive_file.name}".')
                production_datetime = pd.Timestamp(year=(pds[24] - 1) * 100 + pds[12],
                                                   month=pds[13], day=pds[14], hour=pds[15])
                yield (offset, length, pds[8], production_datetime, pds[45:49].decode('ascii', 'replace'))

                offset += length

    def _archive_message_is_placeable(self, blob, length) -> bool:
        '''Structural check on an extracted message, before it is written anywhere.'''

        return len(blob) == length and blob[:4] == b'GRIB' and blob[-4:] == b'7777'

    def import_from_archive(self, archive_file:str, overwrite:bool=False, dry_run:bool=False) -> int:
        '''
        Recreates the per-month local files from a concatenated GRIB1 archive, as if they
        had been retrieved from CDS.

        A CDS monthly-means archive is a plain concatenation of the very messages the API
        returns one per request, so messages are copied out byte for byte -- no decoding and
        no re-encoding, which makes the results bit-identical to a download (verified by
        sha256 against real downloads). Only `expver` 0001 is taken, so the preliminary
        ERA5T release of the most recent months never overwrites final data.

        Target paths come from self.data_index, so the path templates are never restated
        here and months outside the task's date range are skipped.

        Returns the number of local files written.
        '''

        self.diag(f'    Importing from "{archive_file}"...', 1)

        expected_parameter = _GRIB1_PARAMETER.get(self._variable)
        if expected_parameter is None:
            raise Exception(f'No GRIB1 parameter known for "{self._variable}" ({self.__class__.__name__}).')

        # Keyed on the index's own local_file string: base joins the root with a literal
        # '/', so a str(Path(...)) round trip would no longer match it on Windows.
        wanted = {pd.Timestamp(p): l for p, l
                  in self.data_index[['production_datetime', 'local_file']].itertuples(index=False)}

        written = skipped = preliminary = outside = faulty = 0
        with open(archive_file, 'rb') as source:
            for offset, length, parameter, production_datetime, expver in self._grib1_messages(archive_file):
                if parameter != expected_parameter:
                    raise Exception(f'"{Path(archive_file).name}" holds parameter {parameter} at offset '
                                    f'{offset}, expected {expected_parameter} for "{self._variable}" '
                                    f'({self.__class__.__name__}).')

                local_file = wanted.get(production_datetime)
                if local_file is None:
                    outside += 1
                    continue
                local_path = Path(local_file)

                if self._in_faulty_period(production_datetime):
                    self.diag(f'        Skipping {production_datetime.strftime("%Y-%m")}: inside the '
                              f'known-bad window.', 2)
                    faulty += 1
                    continue

                if expver != _FINAL_EXPVER:
                    self.diag(f'        Skipping {production_datetime.strftime("%Y-%m")}: preliminary '
                              f'data (expver {expver}).', 2)
                    preliminary += 1
                    continue

                if not overwrite and local_path.exists() and local_path.stat().st_size == length \
                        and self._grib_looks_intact(local_path):
                    skipped += 1
                    continue

                if dry_run:
                    self.diag(f'        Would write {local_path.name}.', 2)
                    written += 1
                    continue

                source.seek(offset)
                blob = source.read(length)
                if not self._archive_message_is_placeable(blob, length):
                    raise Exception(f'Message for {production_datetime.strftime("%Y-%m")} at offset {offset} '
                                    f'is not a complete GRIB ({self.__class__.__name__}).')

                # Same contract as _download_cds_month: nothing incomplete is ever visible
                # under the final name. The '.part' suffix also keeps the temporary file out
                # of the '*.grib' rglob that base uses to detect local files.
                local_path.parent.mkdir(parents=True, exist_ok=True)
                partial_path = local_path.with_name(local_path.name + '.part')
                with open(partial_path, 'wb') as target:
                    target.write(blob)
                os.replace(partial_path, local_path)

                self.data_index.loc[self.data_index['local_file'] == local_file, 'local_file_exists'] = True
                written += 1

        self.diag(f'        {"Would write" if dry_run else "Wrote"} {written}, kept {skipped} already present, '
                  f'skipped {preliminary} preliminary, {faulty} known-bad and {outside} outside '
                  f'the date range.', 1)

        return written

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
            if values.ndim == 3 and values.shape[0] != 1:
                # Rejected here on purpose: store() reads local files unguarded, so a
                # bare reshape ValueError there would abort every subsequent run.
                raise Exception(f'Field carries a non-unit leading dimension '
                                f'{tuple(ds[variable].dims)}={values.shape}; expected a single monthly field.')
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

        self._reject_faulty_period(local_file)

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

        # Deliberately outside the try below: a known-bad month must surface as an error,
        # not be swallowed into the discard-and-unlink path.
        self._reject_faulty_period(local_file)

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

    # cloud_upload_local/sync_latest_stored are True in .env, so without these a
    # diagnostic run publishes the gribs to Azure and the ~123 MB year files to Dropbox.
    kwargs = dict(download_from_origin=False,
                  date_from='2025-01-01',
                  cloud_upload_local=False,
                  sync_latest_stored=False)

    task = ERA5M_T2M_WORLD(**kwargs)
    # task = ERA5M_TP_WORLD(**kwargs)
    # task = ERA5M_SD_WORLD(**kwargs)
    task.update()

    # World grid: central_longitude must be explicit. The default None reaches
    # ccrs.PlateCarree(central_longitude=None) and cartopy's antimeridian wrapping path
    # then either raises TypeError or degenerates into a per-cell PolyCollection over
    # 6.5M cells. Cropping off the boundary also keeps the fast GeoQuadMesh path.
    # mr = MeteoRaster.load(task.data_index['stored_file'].unique()[-1], dtype='float32')
    # mr.get_cropped(from_lat=-85, to_lat=85, from_lon=-179, to_lon=179).plot_mean(
    #     coastline=True, borders=True, central_longitude=0)
    # mr.get_values_from_latlon(47, 8).plot()

    # docker-compose run --rm tethys-tasks ERA5M_T2M_WORLD update --class_kwargs "{\"download_from_origin\": \"True\", \"date_from\": \"'2025-06-01'\"}"

    pass
