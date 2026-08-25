import inspect

import pandas as pd
import pytest

from tethys_tasks.base import BaseTask
import tethys_tasks.c3s as c3s_module


def _world_c3s_classes():
    classes = []
    for _, cls in inspect.getmembers(c3s_module, inspect.isclass):
        if cls.__module__ != c3s_module.__name__:
            continue
        if not issubclass(cls, BaseTask):
            continue
        if cls.__name__.endswith("_WORLD"):
            classes.append(cls)
    return sorted(classes, key=lambda c: c.__name__)


WORLD_CLASSES = tuple(_world_c3s_classes())


def _leadtime_signature(leadtimes):
    signature = []
    for leadtime in leadtimes:
        if isinstance(leadtime, pd.DateOffset):
            months = int(leadtime.kwds.get("months", 0))
            months += 12 * int(leadtime.kwds.get("years", 0))
            signature.append(months)
        elif isinstance(leadtime, pd.Timedelta):
            signature.append(int(leadtime / pd.Timedelta(days=1)))
        else:
            raise AssertionError(f"Unsupported leadtime type: {type(leadtime)!r}")
    return tuple(signature)


def _build_task(task_cls, date_from):
    return task_cls(
        download_from_origin=True,
        date_from=date_from,
        # Tests must not touch the shared Dropbox account: update() would sync and
        # prune real remote files. Connectivity is covered by test_dropbox_connection.
        sync_latest_stored=False,
    )

def test_world_class_pixel_sizes_and_leadtimes_match():
    assert WORLD_CLASSES, "No C3S _WORLD classes were found in tethys_tasks.c3s"

    date_from = (pd.Timestamp.utcnow().tz_localize(None) - pd.DateOffset(months=3)).strftime("%Y-%m-%d")

    reference_pixels = None
    reference_leadtimes = None

    for task_cls in WORLD_CLASSES:
        task = _build_task(
            task_cls,
            date_from=date_from,
        )

        expected_start = pd.date_range("1900-01-01", pd.Timestamp(date_from), freq=task._production_frequency)[-1]
        assert task.data_index["production_datetime"].min() == expected_start

        pixel_x = float(task._pixel_size)
        pixel_y = float(task._pixel_size)
        assert pixel_x == pixel_y

        leadtimes = _leadtime_signature(task._leadtimes)

        if reference_pixels is None:
            reference_pixels = (pixel_x, pixel_y)
            reference_leadtimes = tuple(leadtimes)
        else:
            assert (pixel_x, pixel_y) == reference_pixels
            assert leadtimes == reference_leadtimes


@pytest.mark.parametrize("task_cls", WORLD_CLASSES, ids=lambda cls: cls.__name__)
def test_retrieve_store_and_upload_real_for_all_world_classes(task_cls, tmp_path):

    if getattr(task_cls, "ARCHIVE_ONLY", False):
        # CDS answers 400 for a retired system version, so asking it for recent data can only
        # fail. The archive stays readable and is covered by the offline tests.
        pytest.skip(f"{task_cls.__name__} is archive-only: CDS no longer publishes this system")

    date_from = (pd.Timestamp.utcnow().tz_localize(None) - pd.DateOffset(months=3)).strftime("%Y-%m-%d")

    task = _build_task(
        task_cls,
        date_from=date_from,
    )

    task.update()

    expected_start = pd.date_range("1900-01-01", pd.Timestamp(date_from), freq=task._production_frequency)[-1]
    assert task.data_index["production_datetime"].min() == expected_start

    # Real run should leave at least one evidence of retrieved or stored data in the index.
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
    else:
        available_leadtimes = task.data_index.loc[task.data_index["data_exists"], "leadtime"].drop_duplicates().to_list()
        if available_leadtimes:
            assert _leadtime_signature(available_leadtimes) == expected_leadtimes
