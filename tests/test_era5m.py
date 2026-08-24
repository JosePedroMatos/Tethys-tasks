import inspect

import pandas as pd
import pytest

from tethys_tasks.base import BaseTask
import tethys_tasks.era5m as era5m_module


def _world_era5m_classes():
    classes = []
    for _, cls in inspect.getmembers(era5m_module, inspect.isclass):
        if cls.__module__ != era5m_module.__name__:
            continue
        if not issubclass(cls, BaseTask):
            continue
        if cls.__name__.endswith("_WORLD"):
            classes.append(cls)
    return sorted(classes, key=lambda c: c.__name__)


WORLD_CLASSES = tuple(_world_era5m_classes())


def _build_task(task_cls, **kwargs):
    # sync_latest_stored=False: tests must not touch the shared Dropbox account,
    # since update() would sync and prune real remote files. Connectivity is
    # covered by test_dropbox_connection.
    kwargs.setdefault('sync_latest_stored', False)
    return task_cls(download_from_origin=True, verbose=0, **kwargs)


def test_world_classes_are_discovered():
    assert {c.__name__ for c in WORLD_CLASSES} == {
        "ERA5M_SD_WORLD",
        "ERA5M_T2M_WORLD",
        "ERA5M_TP_WORLD",
    }


@pytest.mark.parametrize("task_cls", WORLD_CLASSES, ids=lambda cls: cls.__name__)
def test_default_construction_does_not_raise(task_cls):
    # BaseTask's default DATE_FROM (utcnow-7d) leaves populate() with an empty
    # range once PUBLICATION_LATENCY exceeds it, and populate() is not guarded
    # against that. ERA5M sets DATE_FROM to keep this constructor usable, which
    # the acquisition_status reporting pattern relies on.
    task = task_cls(verbose=0)
    assert not task.data_index.empty


@pytest.mark.parametrize("task_cls", WORLD_CLASSES, ids=lambda cls: cls.__name__)
def test_index_is_monthly_and_grouped_by_year(task_cls):
    task = _build_task(task_cls, date_from="2024-03-01", date_to="2025-02-28")

    production_datetimes = pd.DatetimeIndex(task.data_index["production_datetime"].unique())

    # One row per month, anchored on month starts.
    assert (production_datetimes.day == 1).all()
    assert (production_datetimes.hour == 0).all()
    assert len(task.data_index) == len(production_datetimes)

    # Single zero leadtime.
    assert task.data_index["leadtime"].unique().tolist() == [pd.Timedelta(0)]

    # World product: no storage crop.
    assert task.storage_bounding_box is None
    assert task.source_bounding_box == dict(north=90, west=-180, south=-90, east=180)

    # One local grib per month, one stored .nct per calendar year.
    assert task.data_index["local_file"].nunique() == len(production_datetimes)
    stored = task.data_index.groupby(task.data_index["production_datetime"].dt.year)["stored_file"].nunique()
    assert (stored == 1).all()
    assert task.data_index["stored_file"].nunique() == production_datetimes.year.nunique()

    sample = task.data_index.iloc[0]
    assert sample["local_file"].endswith("era5m_%s_2024.03.grib" % task_cls.VARIABLE)
    # Storage is flat under ERA5Land/Monthly: one file per year, so no year folder.
    assert sample["stored_file"].endswith(
        "ERA5Land/Monthly/era5m_{v}_world/tethys_era5m_{v}_2024.nct".format(v=task_cls.VARIABLE))


@pytest.mark.parametrize("task_cls", WORLD_CLASSES, ids=lambda cls: cls.__name__)
def test_storage_search_window_spans_a_year(task_cls):
    # store() extends the index by this window before grouping by stored_file, so
    # anything shorter than a year silently truncates the yearly file.
    task = _build_task(task_cls, date_from="2024-06-01", date_to="2024-06-30")
    reference = pd.Timestamp("2024-06-01")
    assert reference - task._storage_search_window <= pd.Timestamp("2024-01-01")
    assert reference + task._storage_search_window >= pd.Timestamp("2024-12-01")


@pytest.mark.parametrize("task_cls", WORLD_CLASSES, ids=lambda cls: cls.__name__)
def test_faulty_period_covers_accumulated_variables_only(task_cls):
    # ERA5-Land monthly means carry wrong accumulated variables from Sep 2022 to Feb 2024.
    # The fault is invisible in the grib headers, so the window is hardcoded and must stay
    # exact: an off-by-one month either stores bad data or discards good data.
    task = _build_task(task_cls, date_from="2021-01-01", date_to="2025-12-31")
    accumulated = task_cls.VARIABLE in task_cls.FAULTY_PERIOD_VARIABLES

    assert task._in_faulty_period("2022-09-01") is accumulated
    assert task._in_faulty_period("2023-06-01") is accumulated
    assert task._in_faulty_period("2024-02-01") is accumulated
    # the months either side are good for every variable
    assert task._in_faulty_period("2022-08-01") is False
    assert task._in_faulty_period("2024-03-01") is False


@pytest.mark.parametrize("task_cls", WORLD_CLASSES, ids=lambda cls: cls.__name__)
def test_retrieve_store_and_upload_real(task_cls):
    date_from = (pd.Timestamp.utcnow().tz_localize(None) - pd.DateOffset(months=3)).strftime("%Y-%m-%d")

    task = _build_task(task_cls, date_from=date_from)
    task.update()

    assert (
        task.data_index["data_exists"].any()
        or task.data_index["local_file_exists"].any()
        or task.data_index["stored_file_exists"].any()
    )

    local_files = task.data_index.loc[task.data_index["local_file_exists"], "local_file"].unique()
    if len(local_files) > 0:
        sample = task.read_local(local_files[0])
        assert sample.data.ndim == 5
        assert sample.data.shape[:3] == (1, 1, 1)
        assert list(sample.leadtimes) == [pd.Timedelta(0)]
        # No area was requested, so MeteoRaster must have rolled 0..360 to -180..180.
        assert sample.longitudes.min() < 0
