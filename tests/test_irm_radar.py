'''
Offline tests for the IRM radar drivers: no network, no T:\\ drive.

Every task is built with an explicit origin_folder/transfer_folder, so the tests do not depend on
the environment. The real ODIM fixtures in tests/data/IRM_RADAR exercise the actual file layout;
the synthetic ones (small grid, via the GRID override) exercise the QPE hourly resampling.
'''

import tarfile
from pathlib import Path

import h5py
import numpy as np
import pandas as pd
import pytest
from meteoraster import MeteoRaster

from tethys_tasks.base import BaseTask
from tethys_tasks.irm_radar import BESTQPE2_TP, IRM_RADAR, QPE_TP, RADCLIM_TP

DATA = Path(__file__).parent / 'data' / 'IRM_RADAR'
RADCLIM_FIXTURE = DATA / '20210704160000.radclim.1h.hdf'
BESTQPE2_FIXTURE = DATA / '20250603120000.radqpe2.accum1h.hdf'
QPE_FIXTURE = DATA / '20170101000000.rad.best.comp.rate.qpe.hdf'

SMALL_GRID = dict(ul_x=300000.0, ul_y=1000000.0, xscale=1000.0, yscale=1000.0, xsize=4, ysize=3)


def _build(task_cls, origin, tmp_path, **kwargs):
    options = dict(origin_folder=str(origin),
                   transfer_folder=str(tmp_path / 'storage'),
                   verbose=0)
    options.update(kwargs)
    return task_cls(**options)


def _place(origin, task_cls, timestamp, fixture):
    '''Copies a fixture to the path the driver expects for that step.'''

    target = Path(origin) / pd.Timestamp(timestamp).strftime(task_cls.LOCAL_PATH_TEMPLATE)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(Path(fixture).read_bytes())

    return target


def _write_odim(path, array, quantity='RATE', grid=SMALL_GRID):
    '''Minimal ODIM_H5 file with the groups/attributes the driver reads.'''

    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with h5py.File(path, 'w') as handle:
        where = handle.create_group('dataset1/where')
        where.attrs['UL_x'] = grid['ul_x']
        where.attrs['UL_y'] = grid['ul_y']
        where.attrs['xscale'] = grid['xscale']
        where.attrs['yscale'] = grid['yscale']
        where.attrs['xsize'] = grid['xsize']
        where.attrs['ysize'] = grid['ysize']
        handle.create_group('dataset1/data1/what').attrs['quantity'] = np.bytes_(quantity)
        handle.create_dataset('dataset1/data1/data', data=np.asarray(array, dtype='float32'))

    return path


def _qpe_tar(tar_path, samples, quantity='RATE'):
    '''
    Builds a tar of synthetic 5-min QPE members. ``samples`` maps a timestamp to its constant
    field value; the member layout mirrors the real archive.
    '''

    tar_path = Path(tar_path)
    tar_path.parent.mkdir(parents=True, exist_ok=True)
    staging = tar_path.parent / 'staging'
    staging.mkdir(parents=True, exist_ok=True)

    with tarfile.open(tar_path, 'w') as archive:
        for timestamp, value in samples.items():
            timestamp = pd.Timestamp(timestamp)
            member = timestamp.strftime('mnt/HDS_RADAR_EDP/realtime/%Y/%m/%d/best/comp/rate/qpe/hdf/'
                                        '%Y%m%d%H%M%S.rad.best.comp.rate.qpe.hdf')
            field = np.full((SMALL_GRID['ysize'], SMALL_GRID['xsize']), value, dtype='float32')
            field[0, 0] = np.nan                        # outside radar cover in every sample
            local = _write_odim(staging / Path(member).name, field, quantity=quantity)
            archive.add(local, arcname=member)

    return tar_path


# ---------------------------------------------------------------------------- classes
def test_classes_are_registered():
    import tethys_tasks

    for name in ('RADCLIM_TP', 'QPE_TP', 'BESTQPE2_TP'):
        assert name in tethys_tasks.__all__
        assert issubclass(getattr(tethys_tasks, name), BaseTask)


@pytest.mark.parametrize('task_cls', [RADCLIM_TP, QPE_TP, BESTQPE2_TP])
def test_hourly_leadtime_zero_and_monthly_storage(task_cls, tmp_path):
    task = _build(task_cls, tmp_path / 'origin', tmp_path,
                  date_from='2021-07-05 00:00:00', date_to='2021-07-05 03:00:00')

    index = task.data_index
    assert list(pd.to_timedelta(index['leadtime'].unique())) == [pd.Timedelta('0h')]
    assert index['production_datetime'].tolist() == list(pd.date_range('2021-07-05', periods=4, freq='1h'))
    # One .nct per month, and every source path inside the origin.
    assert index['stored_file'].nunique() == 1
    assert index['stored_file'].iloc[0].endswith('2021.07.01.nct')
    assert all(str(tmp_path / 'origin') in path for path in index['local_file'])


def test_origin_folder_is_a_parameter(tmp_path):
    task = _build(RADCLIM_TP, tmp_path / 'elsewhere', tmp_path,
                  date_from='2021-07-05 00:00:00', date_to='2021-07-05 00:00:00')

    assert task._local_storage_folder == str(tmp_path / 'elsewhere')
    assert task.data_index['local_file'].iloc[0] == (
        f'{tmp_path / "elsewhere"}/radclim/2021/07/05/20210705000000.radclim.1h.hdf')


def test_write_paths_into_the_origin_are_disabled(tmp_path):
    task = _build(RADCLIM_TP, tmp_path / 'origin', tmp_path,
                  date_from='2021-07-05 00:00:00', date_to='2021-07-05 00:00:00')

    assert task._download_from_source() is False
    assert task._download_from_cloud() is False
    assert task._cleanup_old_files() is None
    assert task._check_cloud(['a', 'b']) == [False, False]
    assert task._sync_latest_stored is False and task._cloud_upload_local is False


# ---------------------------------------------------------------------------- grid
def test_grid_matches_the_file_corners(tmp_path):
    task = _build(RADCLIM_TP, tmp_path / 'origin', tmp_path,
                  date_from='2021-07-04 16:00:00', date_to='2021-07-04 16:00:00')
    latitudes, longitudes = task._latlon()

    assert latitudes.shape == (700, 700) == longitudes.shape
    # Row 0 is the northernmost, so MeteoRaster does not flip the grid.
    assert latitudes[0, 0] > latitudes[1, 0]

    with h5py.File(RADCLIM_FIXTURE, 'r') as handle:
        corners = dict(handle['where'].attrs)

    # The file states the outer cell corners; the computed values are cell centres, so they sit
    # half a pixel (~0.005 deg) inside them.
    half_pixel = 0.01
    for (key_lat, key_lon), (lat, lon) in {
        ('UL_lat', 'UL_lon'): (latitudes[0, 0], longitudes[0, 0]),
        ('UR_lat', 'UR_lon'): (latitudes[0, -1], longitudes[0, -1]),
        ('LL_lat', 'LL_lon'): (latitudes[-1, 0], longitudes[-1, 0]),
        ('LR_lat', 'LR_lon'): (latitudes[-1, -1], longitudes[-1, -1]),
    }.items():
        assert abs(float(corners[key_lat]) - float(lat)) < half_pixel
        assert abs(float(corners[key_lon]) - float(lon)) < half_pixel


# ---------------------------------------------------------------------------- reading
@pytest.mark.parametrize('task_cls, fixture, timestamp', [
    (RADCLIM_TP, RADCLIM_FIXTURE, '2021-07-04 16:00:00'),
    (BESTQPE2_TP, BESTQPE2_FIXTURE, '2025-06-03 12:00:00'),
])
def test_read_local_returns_one_hourly_step(task_cls, fixture, timestamp, tmp_path):
    origin = tmp_path / 'origin'
    local_file = _place(origin, task_cls, timestamp, fixture)
    task = _build(task_cls, origin, tmp_path, date_from=timestamp, date_to=timestamp)

    mr = task.read_local(local_file)

    assert mr.data.shape == (1, 1, 1, 700, 700)
    assert mr.units == 'mm/hr' and mr.variable == 'tp'
    assert list(pd.DatetimeIndex(mr.production_datetime)) == [pd.Timestamp(timestamp)]
    # Roughly a third of the domain is outside radar cover, the rest is valid.
    assert 0.5 < float(np.mean(np.isfinite(mr.data))) < 0.9
    assert np.nanmax(mr.data) > 0


def test_a_foreign_grid_is_rejected(tmp_path):
    origin = tmp_path / 'origin'
    task = _build(RADCLIM_TP, origin, tmp_path,
                  date_from='2021-07-05 00:00:00', date_to='2021-07-05 00:00:00')

    target = Path(origin) / pd.Timestamp('2021-07-05').strftime(RADCLIM_TP.LOCAL_PATH_TEMPLATE)
    _write_odim(target, np.zeros((3, 4)), quantity='ACRR', grid=SMALL_GRID)

    with pytest.raises(Exception, match='Unexpected grid'):
        task._decode(target.read_bytes())


def test_a_foreign_quantity_is_rejected(tmp_path):
    task = _build(RADCLIM_TP, tmp_path / 'origin', tmp_path, grid=SMALL_GRID,
                  date_from='2021-07-05 00:00:00', date_to='2021-07-05 00:00:00')
    source = _write_odim(tmp_path / 'wrong.hdf', np.zeros((3, 4)), quantity='RATE')

    with pytest.raises(Exception, match='Unexpected quantity'):
        task._decode(source.read_bytes())


def test_existing_steps_only_reports_present_files(tmp_path):
    origin = tmp_path / 'origin'
    _place(origin, RADCLIM_TP, '2021-07-05 01:00:00', RADCLIM_FIXTURE)
    task = _build(RADCLIM_TP, origin, tmp_path,
                  date_from='2021-07-05 00:00:00', date_to='2021-07-05 02:00:00')

    task._check_existing_files(stored=False, cloud=False)

    exists = task.data_index.set_index('production_datetime')['local_file_exists']
    assert exists.loc[pd.Timestamp('2021-07-05 01:00:00')]
    assert not exists.loc[pd.Timestamp('2021-07-05 00:00:00')]
    assert not exists.loc[pd.Timestamp('2021-07-05 02:00:00')]


# ---------------------------------------------------------------------------- storage
def test_store_writes_a_nct_and_leaves_the_origin_untouched(tmp_path):
    origin = tmp_path / 'origin'
    _place(origin, RADCLIM_TP, '2021-07-05 01:00:00', RADCLIM_FIXTURE)
    before = sorted(path.relative_to(origin).as_posix() for path in origin.rglob('*'))

    task = _build(RADCLIM_TP, origin, tmp_path,
                  date_from='2021-07-05 01:00:00', date_to='2021-07-05 01:00:00')
    assert task.store() is True

    stored_file = Path(task.data_index['stored_file'].iloc[0])
    assert stored_file.exists()

    mr = MeteoRaster.load(stored_file, verbose=False)
    # The whole month is indexed; only the hour with a source is filled in.
    filled = np.isfinite(mr.data).any(axis=(1, 2, 3, 4))
    assert mr.data.shape[0] == 744 and int(filled.sum()) == 1
    position = list(pd.DatetimeIndex(mr.production_datetime)).index(pd.Timestamp('2021-07-05 01:00:00'))
    assert filled[position]
    np.testing.assert_allclose(mr.data[position, 0, 0, :, :],
                               task.read_local(_place(origin, RADCLIM_TP, '2021-07-05 01:00:00', RADCLIM_FIXTURE)).data[0, 0, 0],
                               equal_nan=True)

    # Nothing was created inside the read-only origin (no completeness.csv, no new folders).
    assert sorted(path.relative_to(origin).as_posix() for path in origin.rglob('*')) == before


def test_store_is_idempotent_and_keeps_stored_steps(tmp_path):
    origin = tmp_path / 'origin'
    _place(origin, RADCLIM_TP, '2021-07-05 01:00:00', RADCLIM_FIXTURE)

    build = lambda: _build(RADCLIM_TP, origin, tmp_path,
                           date_from='2021-07-05 01:00:00', date_to='2021-07-05 01:00:00')
    assert build().store() is True
    stored_file = Path(build().data_index['stored_file'].iloc[0])
    stamp = stored_file.stat().st_mtime_ns

    # A closed archive is marked complete once every available source is in, so a re-run is a no-op.
    assert build().store() is False
    assert stored_file.stat().st_mtime_ns == stamp


# ---------------------------------------------------------------------------- QPE resampling
def _qpe_task(tmp_path, samples, **kwargs):
    origin = tmp_path / 'origin'
    _qpe_tar(origin / 'QPE' / 'QPE_test.tar', samples)
    options = dict(grid=SMALL_GRID, tar_index_folder=str(tmp_path / 'cache'),
                   date_from='2017-01-01 01:00:00', date_to='2017-01-01 01:00:00')
    options.update(kwargs)

    return _build(QPE_TP, origin, tmp_path, **options)


def test_qpe_hour_is_the_mean_of_its_five_minute_samples(tmp_path):
    # (00:05 .. 01:00] -> the twelve samples of the hour ending at 01:00.
    samples = {pd.Timestamp('2017-01-01 00:05') + i * pd.Timedelta('5min'): float(i)
               for i in range(12)}
    # A sample outside the hour must not be used.
    samples[pd.Timestamp('2017-01-01 01:05')] = 1000.0
    task = _qpe_task(tmp_path, samples)

    array = task._read_step(pd.Timestamp('2017-01-01 01:00'))

    assert array.shape == (SMALL_GRID['ysize'], SMALL_GRID['xsize'])
    assert array.dtype == np.dtype('float32')
    np.testing.assert_allclose(array[1, 1], np.mean(np.arange(12.0)))
    # A pixel that is NaN in every sample stays NaN.
    assert np.isnan(array[0, 0])


def test_qpe_drops_hours_with_too_few_samples(tmp_path):
    samples = {pd.Timestamp('2017-01-01 00:05') + i * pd.Timedelta('5min'): 1.0 for i in range(9)}
    task = _qpe_task(tmp_path, samples)

    assert task._read_step(pd.Timestamp('2017-01-01 01:00')) is None
    steps = task._existing_steps(pd.DatetimeIndex([pd.Timestamp('2017-01-01 01:00')]))
    assert not steps.iloc[0]

    # The same hour is accepted once the threshold is lowered.
    tolerant = _qpe_task(tmp_path, samples, min_samples=9)
    assert tolerant._read_step(pd.Timestamp('2017-01-01 01:00')) is not None


def test_qpe_reads_a_real_member_from_a_tar(tmp_path):
    origin = tmp_path / 'origin'
    tar_path = origin / 'QPE' / 'QPE_real.tar'
    tar_path.parent.mkdir(parents=True, exist_ok=True)
    with tarfile.open(tar_path, 'w') as archive:
        for i in range(12):
            timestamp = pd.Timestamp('2017-01-01 00:05') + i * pd.Timedelta('5min')
            archive.add(QPE_FIXTURE, arcname=timestamp.strftime(
                'mnt/HDS_RADAR_EDP/realtime/%Y/%m/%d/best/comp/rate/qpe/hdf/'
                '%Y%m%d%H%M%S.rad.best.comp.rate.qpe.hdf'))

    task = _build(QPE_TP, origin, tmp_path, tar_index_folder=str(tmp_path / 'cache'),
                  date_from='2017-01-01 01:00:00', date_to='2017-01-01 01:00:00')

    with h5py.File(QPE_FIXTURE, 'r') as handle:
        expected = np.asarray(handle['dataset1/data1/data'][...], dtype='float32')

    array = task._read_step(pd.Timestamp('2017-01-01 01:00'))
    np.testing.assert_allclose(array, expected, equal_nan=True, rtol=1e-6)
    assert task._existing_steps(pd.DatetimeIndex([pd.Timestamp('2017-01-01 01:00')])).iloc[0]


def test_qpe_tar_index_is_cached_and_reused(tmp_path):
    samples = {pd.Timestamp('2017-01-01 00:05') + i * pd.Timedelta('5min'): 1.0 for i in range(12)}
    cache = tmp_path / 'cache'
    task = _qpe_task(tmp_path, samples, tar_index_folder=str(cache))

    index = task._tar_index()
    caches = list((cache / 'IRM_QPE_TAR_INDEX').glob('*.parquet'))
    assert len(caches) == 1
    assert len(index) == len(samples)

    # A second instance must not rescan: the cached index alone answers the lookups.
    again = _build(QPE_TP, tmp_path / 'origin', tmp_path, grid=SMALL_GRID,
                   tar_index_folder=str(cache),
                   date_from='2017-01-01 01:00:00', date_to='2017-01-01 01:00:00')
    pd.testing.assert_frame_equal(again._tar_index(), index)


def test_irm_radar_base_is_not_a_product():
    # The shared base carries no templates; only the products below it are usable.
    assert IRM_RADAR.LOCAL_PATH_TEMPLATE == ''
    assert IRM_RADAR.STORAGE_PATH_TEMPLATE == ''
