from __future__ import annotations

from tethys_tasks import BaseTask, CaptureNewVariables, DownloadMonitor, create_kml_classes
import boto3
import botocore.config as config_module
import os
import pandas as pd
import xarray as xr
from pathlib import Path
import shutil
import tempfile
from meteoraster import MeteoRaster
import numpy as np
from concurrent.futures import ThreadPoolExecutor, as_completed
from uuid import uuid4


class CLMS_SWE_NH_V2(BaseTask):
	'''
	Copernicus Land Monitoring Service Snow Water Equivalent daily product.

	Product page:
	https://land.copernicus.eu/en/products/snow/snow-water-equivalent-v2-0-5km
	'''

	with CaptureNewVariables() as _CLMS_SWE_NH_V2_VARIABLES: #It is essential that the format of the variable here is _CLASSnAME_VARIABLES
		PUBLICATION_LATENCY = pd.Timedelta(hours=12)
		PRODUCTION_FREQUENCY = pd.Timedelta(days=1)
		LEADTIMES = pd.timedelta_range('0D', '0D', freq='1D')

		SOURCE_PARALLEL_TRANSFERS = 2
		ASSUME_LOCAL_COMPLETE = True

		DATE_FROM = '2024-07-01'
		FAIL_IF_OLDER = pd.Timedelta(days=3)

		VARIABLE = 'swe'
		UNITS = 'mm'
		PIXEL_SIZE = 0.05
		MISSING_FUNCTION = lambda x: x<=-10
		DATA_TRANSFORM_FUNCTION = lambda x: x

		DATASET_IDENTIFIER = 'swe_northernhemisphere_5km_daily_v2'
		FILE_FORMAT = 'nc'
		CATALOGUE_URL = 'https://s3.waw3-1.cloudferro.com/swift/v1/CatalogueCSV/bio-geophysical/snow_water_equivalent/swe_northernhemisphere_5km_daily_v2/swe_northernhemisphere_5km_daily_v2_nc.csv'
		S3_ENDPOINT_URL = os.getenv('CDSE_S3_ENDPOINT_URL')
		S3_REGION_NAME = os.getenv('CDSE_S3_REGION_NAME')
		S3_BUCKET = os.getenv('CDSE_S3_BUCKET')
		S3_ACCESS_KEY = os.getenv('CDSE_S3_ACCESS_KEY')
		S3_SECRET_KEY = os.getenv('CDSE_S3_SECRET_KEY')

		ZONE = 'northern_hemisphere'
		SOURCE_KML = ''
		STORAGE_KML = ''

		CLOUD_TEMPLATE = 'CLMS/SWE_NH_V2/%Y/%m/clms_swe_%Y.%m.%d.nc'
		LOCAL_PATH_TEMPLATE = 'CLMS/SWE_NH_V2/%Y/%m/clms_swe_%Y.%m.%d.nc'
		STORAGE_PATH_TEMPLATE = 'CLMS/SWE_NH_V2/{self._zone}/%Y/tethys_clms_swe_%Y.%m.nct'

		STORAGE_SEARCH_WINDOW = pd.DateOffset(days=40)

	def __init__(self, *args, **kwargs):
		super().__init__(*args, **kwargs)
		self._catalogue = None
		self._s3_client = None

	def _get_s3_client(self):
		'''
		Returns a boto3 S3 client for the CDSE S3 endpoint, using credentials from .env
		'''
		if self._s3_client is not None:
			return self._s3_client

		if not self._s3_access_key or not self._s3_secret_key:
			raise RuntimeError('CLMS SWE download requires CDSE S3 credentials. Set CDSE_S3_ACCESS_KEY and CDSE_S3_SECRET_KEYin .env.')

		self._s3_client = boto3.client(
			's3',
			endpoint_url=self._s3_endpoint_url,
			aws_access_key_id=self._s3_access_key,
			aws_secret_access_key=self._s3_secret_key,
			region_name=self._s3_region_name,
			config=config_module.Config(signature_version='s3v4'),
		)
		return self._s3_client

	def _get_catalogue(self) -> pd.DataFrame:
		'''
		Returns a catalogue of available data before download
		'''
		if self._catalogue is not None:
			return self._catalogue

		catalogue = pd.read_csv(self._catalogue_url, sep=';')
		catalogue['nominal_date'] = pd.to_datetime(catalogue['nominal_date']).dt.tz_localize(None)
		catalogue['modification_date'] = pd.to_datetime(catalogue['modification_date']).dt.tz_localize(None)
		catalogue = catalogue.sort_values(['nominal_date', 'modification_date'])
		catalogue = catalogue.drop_duplicates(subset=['nominal_date'], keep='last')
		catalogue = catalogue.rename(columns={'id': 'product_id', 'name': 'product_name'})
		catalogue['s3_key'] = catalogue['s3_path'].str.replace(r'^s3://[^/]+/', '', regex=True)
		catalogue['production_datetime'] = catalogue['nominal_date'].dt.normalize()
		self._catalogue = catalogue
		return self._catalogue

	def _download_file(self, info: dict) -> tuple[str, str]:
		destination = Path(info['local_file'])
		destination.parent.mkdir(parents=True, exist_ok=True)
		temp_path = None
		try:
			s3_client = self._get_s3_client()
			resolved_key = self._resolve_s3_key(info)
			with tempfile.NamedTemporaryFile(delete=False, suffix=f'.{uuid4().hex}.part') as handle:
				temp_path = Path(handle.name)
				s3_client.download_fileobj(self._s3_bucket, resolved_key, handle)

			shutil.move(temp_path, destination)
			return ('Downloaded', info['local_file'])
		except Exception as ex:
			if temp_path is not None and temp_path.exists():
				temp_path.unlink()
			print(f'        Error downloading {resolved_key} -> {destination}: {ex}.')
			return ('Failed', info['local_file'])

	def _resolve_s3_key(self, info: dict) -> str:
		base_key = str(info['s3_key']).rstrip('/')
		s3_client = self._get_s3_client()

		candidates = [base_key]
		product_name = str(info.get('product_name', '')).strip('/')
		if product_name:
			candidates.append(f'{base_key}/{product_name}')
			if product_name.endswith('_nc'):
				candidates.append(f"{base_key}/{product_name[:-3]}.nc")

		for candidate in dict.fromkeys(candidates):
			try:
				s3_client.head_object(Bucket=self._s3_bucket, Key=candidate)
				return candidate
			except Exception as ex:
				status_code = getattr(getattr(ex, 'response', {}), 'get', lambda *_: None)('Error', {}).get('Code') if hasattr(ex, 'response') else None
				if status_code not in ['NoSuchKey', '404', 'NotFound']:
					raise

		response = s3_client.list_objects_v2(Bucket=self._s3_bucket, Prefix=f'{base_key}/', MaxKeys=20)
		objects = [obj['Key'] for obj in response.get('Contents', []) if not obj['Key'].endswith('/')]
		if len(objects) == 1:
			return objects[0]

		netcdf_objects = [key for key in objects if key.endswith('.nc')]
		if len(netcdf_objects) == 1:
			return netcdf_objects[0]

		raise FileNotFoundError(f"Could not resolve CLMS SWE object from S3 path: {info['s3_key']}")

	def _download_from_source(self) -> bool:
		'''
		Downloads missing CLMS SWE files from the CDSE S3 endpoint.
		'''

		self.diag('    Download from source...', 1)

		missing_dates = pd.Index(self.data_index.loc[~self.data_index['data_exists'], 'production_datetime'].unique())
		if missing_dates.empty:
			self.diag('        Nothing to download.', 1)
			return False

		catalogue = self._get_catalogue()
		available = catalogue.loc[catalogue['production_datetime'].isin(missing_dates), :]
		if available.empty:
			self.diag(f'        No matching products currently available in the CLMS catalogue ({self.__class__.__name__}).', 1)
			return False

		to_download = self.data_index.loc[
			self.data_index['production_datetime'].isin(available['production_datetime']),
		].drop_duplicates().merge(available, on='production_datetime', how='left')

		self.diag(f'        Downloading ({self._source_parallel_transfers} threads).', 1)
		downloaded = False
		with DownloadMonitor() as monitor:
			with ThreadPoolExecutor(max_workers=self._source_parallel_transfers) as executor:
				futures = {
					executor.submit(self._download_file, row.to_dict())
					for _, row in to_download.iterrows()
				}
				for future in as_completed(futures):
					status, local_file = future.result()
					if status == 'Downloaded':
						msg = monitor.mark_success(local_file)
						self.diag('        ' + msg, 1)
						self.data_index.loc[self.data_index['local_file']==local_file, 'local_file_exists'] = True
						downloaded = True
					else:
						self.diag(f'        Download failed for {local_file}.', 1)

		if downloaded:
			self._check_existing_data(stored=False, cloud=False)

		return downloaded

	def read_local(self, local_file: str) -> MeteoRaster:
		'''
		Returns a MeteoRaster object with CLMS SWE data.
		'''

		self.diag(f'            Reading "{local_file}" ({self.__class__.__name__})', 1)

		with xr.open_dataset(local_file, engine=self._engine) as ds:
			data_array = ds[self._variable]

			longitudes = np.round(ds['lon'].values, 3)
			latitudes = np.round(ds['lat'].values, 3)
			data = np.expand_dims(data_array.values[...], (1, 2))
			production_datetime = pd.to_datetime(ds['time'].values)

            
			data[self._missing_function(data)] = np.nan
			data = self._data_transform_function(data)
			
			units = data_array.attrs.get('units')
			if units and self._units and units != self._units:
				raise ValueError(f"        Units mismatch: data has units '{units}' but task is configured for '{self._units}' ({self.__class__.__name__}).")

		payload = {
			'latitudes': latitudes,
			'longitudes': longitudes,
			'production_datetime': production_datetime,
			'leadtimes': pd.to_timedelta([0], unit='D'),
			'data': data
		}

		mr = MeteoRaster(payload, units=self._units, variable=self._variable, verbose=False)
		mr.trim()
		return mr

class CLMS_SWE_NH_V1(CLMS_SWE_NH_V2):
	'''
	Copernicus Land Monitoring Service Snow Water Equivalent daily product, version 1.

	Product page:
	https://land.copernicus.eu/en/products/snow/snow-water-equivalent-v1-0-5km
	'''

	with CaptureNewVariables() as _CLMS_SWE_NH_V1_VARIABLES: #It is essential that the format of the variable here is _CLASSnAME_VARIABLES
		DATE_FROM = '2006-05-20'

		DATASET_IDENTIFIER = 'swe_northernhemisphere_5km_daily_v1'
		CATALOGUE_URL = 'https://s3.waw3-1.cloudferro.com/swift/v1/CatalogueCSV/bio-geophysical/snow_water_equivalent/swe_northernhemisphere_5km_daily_v1/swe_northernhemisphere_5km_daily_v1_nc.csv'

		CLOUD_TEMPLATE = 'CLMS/SWE_NH_V1/%Y/%m/clms_swe_%Y.%m.%d.nc'
		LOCAL_PATH_TEMPLATE = 'CLMS/SWE_NH_V1/%Y/%m/clms_swe_%Y.%m.%d.nc'
		STORAGE_PATH_TEMPLATE = 'CLMS/SWE_NH_V1/{self._zone}/%Y/tethys_clms_swe_%Y.%m.nct'

class CLMS_SCE_GLOBAL_V1(CLMS_SWE_NH_V2):
	'''
	Copernicus Land Monitoring Service Snow Cover Extent daily product, global, version 1.

	Product page:
	https://land.copernicus.eu/en/products/snow/snow-cover-global-v1-0-1km
	'''

	with CaptureNewVariables() as _CLMS_SCE_GLOBAL_V1_VARIABLES: #It is essential that the format of the variable here is _CLASSnAME_VARIABLES
		DATE_FROM = '2025-12-09'
		VARIABLE = 'sce'
		UNITS = 'percent'
		PIXEL_SIZE = 0.01
		MISSING_FUNCTION = lambda x: np.logical_or(x<100, x>200)   
		DATA_TRANSFORM_FUNCTION = lambda x: x-100

		DATASET_IDENTIFIER = 'sce_global_1km_daily_v1'
		CATALOGUE_URL = 'https://s3.waw3-1.cloudferro.com/swift/v1/CatalogueCSV/bio-geophysical/snow_cover_extent/sce_global_1km_daily_v1/sce_global_1km_daily_v1_nc.csv'

		ZONE = 'global'

		CLOUD_TEMPLATE = 'CLMS/SCE_GLOBAL_V1/%Y/%m/clms_sce_%Y.%m.%d.nc'
		LOCAL_PATH_TEMPLATE = 'CLMS/SCE_GLOBAL_V1/%Y/%m/clms_sce_%Y.%m.%d.nc'
		STORAGE_PATH_TEMPLATE = 'CLMS/SCE_GLOBAL_V1/{self._zone}/%Y/tethys_clms_sce_%Y.%m.nct'

class CLMS_SCE_NH_V1(CLMS_SWE_NH_V2):
	'''
	Copernicus Land Monitoring Service Snow Cover Extent daily product, northern hemisphere, version 1.

	Product page:
	https://land.copernicus.eu/en/products/snow/snow-cover-extent-northern-hemisphere-v1-0-1km
	'''

	with CaptureNewVariables() as _CLMS_SCE_NH_V1_VARIABLES: #It is essential that the format of the variable here is _CLASSnAME_VARIABLES
		DATE_FROM = '2018-07-10'
		VARIABLE = 'sce'
		UNITS = 'percent'
		PIXEL_SIZE = 0.01

		DATASET_IDENTIFIER = 'sce_northernhemisphere_1km_daily_v1'
		CATALOGUE_URL = 'https://s3.waw3-1.cloudferro.com/swift/v1/CatalogueCSV/bio-geophysical/snow_cover_extent/sce_northernhemisphere_1km_daily_v1/sce_northernhemisphere_1km_daily_v1_nc.csv'

		ZONE = 'northern_hemisphere'

		CLOUD_TEMPLATE = 'CLMS/SCE_NH_V1/%Y/%m/clms_sce_%Y.%m.%d.nc'
		LOCAL_PATH_TEMPLATE = 'CLMS/SCE_NH_V1/%Y/%m/clms_sce_%Y.%m.%d.nc'
		STORAGE_PATH_TEMPLATE = 'CLMS/SCE_NH_V1/{self._zone}/%Y/tethys_clms_sce_%Y.%m.nct'

create_kml_classes(CLMS_SWE_NH_V2)
create_kml_classes(CLMS_SWE_NH_V1)
create_kml_classes(CLMS_SCE_GLOBAL_V1)
create_kml_classes(CLMS_SCE_NH_V1)

if __name__=='__main__':
	import matplotlib.pyplot as plt
	plt.ion()

	task = CLMS_SWE_NH_V2(download_from_origin=True, date_from='2026-01-01')
	# task = CLMS_SWE_NH_V2_CAUCASUS(download_from_origin=True, date_from='2026-03-01')
	# task = CLMS_SCE_GLOBAL_V1_CAUCASUS(download_from_origin=True, date_from='2026-04-05')
	task.update()

    # files = task.data_index['stored_file'].unique()
    # mr = MeteoRaster.load(files[-1])
    # mr.plot_mean(coastline=True, borders=True)
    # mr.get_values_from_latlon(40, 71).plot()

    # kml = r'C:\Users\zepedro\Universidade de Lisboa\IST-TETHYS - GSE training 2025.09\Shared\SHP\Rioni.kml'
    # data, centroids = mr.get_values_from_KML(kml, nameField='ID')

	pass