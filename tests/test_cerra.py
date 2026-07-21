import inspect

import pandas as pd
import pytest

from tethys_tasks.base import BaseTask
import tethys_tasks.cerra as cerra_module


def _cerra_classes():
    classes = []
    for _, cls in inspect.getmembers(cerra_module, inspect.isclass):
        if cls.__module__ != cerra_module.__name__:
            continue
        if not issubclass(cls, BaseTask):
            continue
        if cls.__name__.startswith("CERRA_"):
            classes.append(cls)
    return sorted(classes, key=lambda c: c.__name__)


ALL_CLASSES = tuple(_cerra_classes())
# Real downloads are large (full European domain, ~GB/month); exercise the full path on
# the smallest region only.
REAL_CLASSES = tuple(c for c in ALL_CLASSES if c.__name__.endswith("_BELGIUM"))

# A month well inside CERRA's published range (CERRA lags by a long time).
DATE_FROM = "2019-01-01"
DATE_TO = "2019-01-31 23:59:59"


def _leadtime_signature(leadtimes):
    signature = []
    for leadtime in pd.to_timedelta(list(leadtimes)):
        signature.append(int(leadtime / pd.Timedelta(hours=1)))
    return tuple(signature)


def _build_task(task_cls):
    return task_cls(
        download_from_origin=True,
        date_from=DATE_FROM,
        date_to=DATE_TO,
    )


def test_cerra_classes_discovered():
    assert ALL_CLASSES, "No CERRA_* classes were found in tethys_tasks.cerra"
    # tp, t2m, sd across the belgium/switzerland/iberia domains must all be present.
    names = {c.__name__ for c in ALL_CLASSES}
    for region in ("BELGIUM", "SWITZERLAND", "IBERIA"):
        for variable in ("TP", "T2M", "SD"):
            assert f"CERRA_{variable}_{region}" in names


def test_class_pixel_sizes_frequency_and_leadtimes_match():
    reference_pixels = None
    reference_leadtimes = None

    for task_cls in ALL_CLASSES:
        task = _build_task(task_cls)

        # 3-hourly reference-time cadence, snapped to the frequency grid.
        expected_start = pd.date_range("1900-01-01", pd.Timestamp(DATE_FROM), freq=task._production_frequency)[-1]
        assert task.data_index["production_datetime"].min() == expected_start

        pixel = float(task._pixel_size)

        # Hourly analysis-like best-guess: single leadtime 0 for every variable.
        leadtimes = _leadtime_signature(task._leadtimes)
        assert leadtimes == (0,)

        if reference_pixels is None:
            reference_pixels = pixel
            reference_leadtimes = leadtimes
        else:
            assert pixel == reference_pixels
            assert leadtimes == reference_leadtimes


@pytest.mark.parametrize("task_cls", REAL_CLASSES, ids=lambda cls: cls.__name__)
def test_retrieve_store_and_upload_real(task_cls):
    task = _build_task(task_cls)

    task.retrieve_store_and_upload()

    expected_start = pd.date_range("1900-01-01", pd.Timestamp(DATE_FROM), freq=task._production_frequency)[-1]
    assert task.data_index["production_datetime"].min() == expected_start

    # A real run should leave at least one evidence of retrieved or stored data.
    assert (
        task.data_index["data_exists"].any()
        or task.data_index["local_file_exists"].any()
        or task.data_index["stored_file_exists"].any()
    )

    expected_leadtimes = _leadtime_signature(task._leadtimes)
    local_files = task.data_index.loc[task.data_index["local_file_exists"], "local_file"].unique()
    if len(local_files) > 0:
        sample = task.read_local(local_files[0])
        assert _leadtime_signature(sample.leadtimes) == expected_leadtimes
        # Native curvilinear grid: 2-D WGS84 lat/lon.
        assert sample.latitudes.ndim == 2
        assert sample.longitudes.ndim == 2
