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

import numpy as np
import pandas as pd
import xarray as xr
from meteoraster import MeteoRaster

from tethys_tasks import BaseTask, CaptureNewVariables, DownloadMonitor, create_kml_classes


_ALL_LEADTIME_HOURS = [*range(0, 79), *range(81, 121, 3)]


class ICON_EU(BaseTask):
	with CaptureNewVariables() as _ICON_EU_VARIABLES:
		PUBLICATION_LATENCY = pd.Timedelta(hours=4)
		PUBLICATION_MEMORY = pd.Timedelta(hours=24)
		PRODUCTION_FREQUENCY = pd.Timedelta(hours=3)
		LEADTIMES = [pd.Timedelta(hours=hour) for hour in _ALL_LEADTIME_HOURS]

		SOURCE_PARALLEL_TRANSFERS = 3
		STORAGE_SEARCH_WINDOW = pd.DateOffset(days=10)
		ASSUME_LOCAL_COMPLETE = True

		VARIABLE = ''
		ZONE = 'world'
		DOWNLOAD_RETRIES = 1
		DOWNLOAD_RETRY_WAIT = 10

		CLOUD_TEMPLATE = 'ICON_EU/{self._variable_upper}/%Y/%m/icon_eu_{self._variable_lower}_%Y%m%dT%H.zip'
		LOCAL_PATH_TEMPLATE = 'ICON_EU/{self._variable_upper}/%Y/%m/icon_eu_{self._variable_lower}_%Y%m%dT%H.zip'
		STORAGE_PATH_TEMPLATE = 'ICON_EU/icon_eu_{self._variable_lower}_{self._zone}/%Y/%m/tethys_icon_eu_{self._variable_lower}_%Y%m%dT%H.nct'

		FAIL_IF_OLDER = pd.Timedelta(hours=12)
		DATE_FROM = (pd.Timestamp.now(datetime.timezone.utc) - pd.Timedelta('2D')).strftime('%Y-%m-%d %H:%M:%S')

		SOURCE_CONFIG = {
			't2m': {
				'folder': 't_2m',
				'suffix': 'T_2M',
				'data_var': 't2m',
				'units': 'C',
			},
			'tp': {
				'folder': 'tot_prec',
				'suffix': 'TOT_PREC',
				'data_var': 'tp',
				'units': 'mm/h',
			},
			'sd': {
				'folder': 'h_snow',
				'suffix': 'H_SNOW',
				'data_var': 'sde',
				'units': 'mm',
			},
		}

	def _member_name(self, production_datetime: pd.Timestamp, leadtime_hour: int) -> str:
		return (
			'icon-eu_europe_regular-lat-lon_single-level_'
			f'{production_datetime:%Y%m%d%H}_{leadtime_hour:03d}_{self._source_config[self._variable]["suffix"]}.grib2.bz2'
		)

	def _source_url(self, production_datetime: pd.Timestamp, leadtime_hour: int) -> str:
		folder = self._source_config[self._variable]['folder']
		return (
			'https://opendata.dwd.de/weather/nwp/icon-eu/grib/'
			f'{production_datetime:%H}/{folder}/{self._member_name(production_datetime, leadtime_hour)}'
		)

	@staticmethod
	def _is_unavailable_error(ex: Exception) -> bool:
		message = str(ex).lower()
		return '404' in message or 'not found' in message

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

		self.diag(
			f'        Downloading ICON-EU with {max(int(self._source_parallel_transfers), 1)} workers and {max(int(self._download_retries), 0)} retries.',
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

			with tempfile.TemporaryDirectory(prefix='icon_eu_') as tmp_dir:
				tmp_path = Path(tmp_dir)
				member_files = {}
				failed = False
				unavailable = False

				with DownloadMonitor() as monitor:
					executor = ThreadPoolExecutor(max_workers=max(int(self._source_parallel_transfers), 1))
					try:
						futures = {
							executor.submit(
								self._download_member,
								production_datetime,
								tmp_path,
								leadtime_hour,
							): leadtime_hour
							for leadtime_hour in _ALL_LEADTIME_HOURS
						}

						for future in as_completed(futures):
							status, member_path, leadtime_hour, detail = future.result()
							if status == 'Downloaded':
								member_files[leadtime_hour] = member_path
								self.diag('        ' + monitor.mark_success(member_path), 1)
								continue

							failed = True
							if status == 'Unavailable':
								unavailable = True
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
					if unavailable:
						continue
					continue

				if len(member_files) != len(_ALL_LEADTIME_HOURS):
					self.diag(
						f'        Skipping zip creation for {production_datetime:%Y-%m-%d %H:%M}; only {len(member_files)}/{len(_ALL_LEADTIME_HOURS)} files completed.',
						1,
					)
					continue

				fd, temp_name = tempfile.mkstemp(prefix=local_file.stem + '.', suffix='.part', dir=local_file.parent)
				os.close(fd)
				temp_zip = Path(temp_name)
				try:
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

	def _read_member(self, member_file: Path) -> tuple[pd.Timestamp, pd.Timedelta, np.ndarray, np.ndarray, np.ndarray]:
		grib_file = member_file.with_suffix('')
		grib_file.write_bytes(bz2.decompress(member_file.read_bytes()))

		try:
			with xr.open_dataset(str(grib_file), engine='cfgrib', indexpath='') as ds:
				data_var = self._source_config[self._variable]['data_var']
				if data_var not in ds.data_vars:
					if len(ds.data_vars) != 1:
						available = ', '.join(sorted(ds.data_vars))
						raise KeyError(f'Variable {data_var} not found in {member_file.name}. Available variables: {available}')
					data_var = next(iter(ds.data_vars))
				data_array = ds[data_var]

				values = np.asarray(data_array.data, dtype=np.float32)
				leadtime = pd.to_timedelta(np.asarray(ds['step'].data).reshape(-1)[0])
				production_datetime = pd.to_datetime(np.asarray(ds['time'].data).reshape(-1))[0]
				latitudes = np.asarray(ds['latitude'].data)
				longitudes = np.asarray(ds['longitude'].data)
		finally:
			if grib_file.exists():
				grib_file.unlink()

		return production_datetime, leadtime, values, latitudes, longitudes

	def read_local(self, local_file: str) -> MeteoRaster:
		self.diag(f'            Reading "{local_file}" ({self.__class__.__name__})', 1)

		with tempfile.TemporaryDirectory(prefix='icon_eu_read_') as tmp_dir:
			tmp_path = Path(tmp_dir)
			with ZipFile(local_file, 'r') as archive:
				archive.extractall(tmp_path)

			member_files = sorted(tmp_path.glob('*.bz2'))
			if not member_files:
				raise RuntimeError(f'No ICON-EU files found in {local_file}.')

			production_datetime = None
			latitudes = None
			longitudes = None
			leadtimes = []
			values = []

			for member_file in member_files:
				file_production_datetime, leadtime, file_values, file_latitudes, file_longitudes = self._read_member(member_file)
				if production_datetime is None:
					production_datetime = file_production_datetime
					latitudes = file_latitudes
					longitudes = file_longitudes
				leadtimes.append(leadtime)
				values.append(file_values)

			leadtimes = pd.to_timedelta(leadtimes)
			order = np.argsort(leadtimes.to_numpy())
			leadtimes = leadtimes[order]
			values = np.stack([values[i] for i in order], axis=0).astype(np.float32)

			if self._variable == 't2m':
				values -= 273.15
			elif self._variable == 'sd':
				values *= 1000.0
			elif self._variable == 'tp':
				leadtime_hours = np.asarray([leadtime / pd.Timedelta(hours=1) for leadtime in leadtimes], dtype=float)
				interval_hours = np.diff(leadtime_hours, prepend=0.0)
				values = np.diff(values, axis=0, prepend=values[:1, ...])
				divisor = interval_hours.copy()
				divisor[0] = 1.0
				values = values * 1.0 / divisor.reshape(-1, 1, 1)
				values[0, :, :] = 0.0
				values = np.maximum(values, 0.0)

			mr_data = {
				'data': values[np.newaxis, np.newaxis, ...],
				'production_datetime': pd.to_datetime([production_datetime]),
				'leadtimes': leadtimes,
				'latitudes': latitudes,
				'longitudes': longitudes,
			}
			return MeteoRaster(
				data=mr_data,
				units=self._source_config[self._variable]['units'],
				variable=self._variable,
				verbose=False,
			)

	def read_local_completeness(self, local_file: str) -> pd.Series:
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


class ICON_EU_T2M_WORLD(ICON_EU):
	with CaptureNewVariables() as _ICON_EU_T2M_WORLD_VARIABLES:
		VARIABLE = 't2m'
		ZONE = 'world'


class ICON_EU_TP_WORLD(ICON_EU):
	with CaptureNewVariables() as _ICON_EU_TP_WORLD_VARIABLES:
		VARIABLE = 'tp'
		ZONE = 'world'


class ICON_EU_SD_WORLD(ICON_EU):
	with CaptureNewVariables() as _ICON_EU_SD_WORLD_VARIABLES:
		VARIABLE = 'sd'
		ZONE = 'world'


create_kml_classes(ICON_EU, {'VARIABLE': ['tp', 't2m', 'sd']})


if __name__ == '__main__':
	import matplotlib.pyplot as plt
	plt.ion()

	task_cls = globals().get('ICON_EU_T2M_IBERIA')
	if task_cls is None:
		raise RuntimeError('ICON_EU_T2M_IBERIA was not created at runtime.')

	task = task_cls(
		date_from='2026-04-16 00:00:00',
		download_from_source=False,
	)
	task.update()
