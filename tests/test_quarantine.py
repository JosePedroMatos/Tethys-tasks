import tarfile
import zipfile

import pandas as pd
import pytest

from tethys_tasks import BaseTask
from tethys_tasks.base import _is_corrupt_local_file
from tethys_tasks.functions import CompletenessIndex


class _FakeGribError(Exception):
    pass


@pytest.mark.parametrize('ex, expected', [
    (EOFError('No valid message found'), True),
    (KeyError('distinctLatitudes'), True),
    (zipfile.BadZipFile('File is not a zip file'), True),
    (tarfile.ReadError('unexpected end of data'), True),
    (Exception('Leadtimes do not all match (base: 61, joint: 35). Error issued in strickt mode.'), True),
    (ValueError('some genuine bug'), False),
    (TypeError('bad argument'), False),
    (AttributeError('typo in driver'), False),
])
def test_is_corrupt_local_file_classification(ex, expected):
    assert _is_corrupt_local_file(ex) is expected


def test_is_corrupt_local_file_accepts_gribapi_errors():
    import eccodes  # noqa: F401  -- import order matters, gribapi alone can cycle
    from gribapi.errors import KeyValueNotFoundError, DecodingError, PrematureEndOfFileError

    for cls in (KeyValueNotFoundError, DecodingError, PrematureEndOfFileError):
        assert _is_corrupt_local_file(cls(1)) is True


def test_is_corrupt_local_file_ignores_unrelated_module_errors():
    # Guards the module-name check from being over-broad.
    assert _is_corrupt_local_file(_FakeGribError('looks scary, is not')) is False


def _task_with_local_file(local_file):
    bt = BaseTask()
    bt.verbose = 0
    bt.data_index = pd.DataFrame({
        'local_file': [str(local_file)],
        'local_file_exists': [True],
        'local_file_complete': [True],
    })
    return bt


def test_quarantine_renames_and_clears_completeness(tmp_path):
    local_file = tmp_path / 'ipma_t2m_20260824.grib'
    local_file.write_bytes(b'GRIB truncated')

    ci = CompletenessIndex(tmp_path)
    ci.include([local_file.name])
    ci.write()
    assert (tmp_path / 'completeness.csv').exists()

    bt = _task_with_local_file(local_file)
    bt._quarantine_local_file(str(local_file), 'EOFError: No valid message found')

    # Renamed, never deleted.
    assert not local_file.exists()
    assert (tmp_path / 'ipma_t2m_20260824.grib.corrupt').read_bytes() == b'GRIB truncated'

    # Dropped from the sidecar and from the index, so the next run re-downloads it.
    assert local_file.name not in CompletenessIndex(tmp_path).get_complete()
    assert not bt.data_index.loc[0, 'local_file_exists']
    assert not bt.data_index.loc[0, 'local_file_complete']


def test_quarantine_overwrites_an_earlier_corrupt_file(tmp_path):
    local_file = tmp_path / 'run.grib'
    local_file.write_bytes(b'second bad download')
    (tmp_path / 'run.grib.corrupt').write_bytes(b'first bad download')

    bt = _task_with_local_file(local_file)
    bt._quarantine_local_file(str(local_file), 'EOFError')

    assert (tmp_path / 'run.grib.corrupt').read_bytes() == b'second bad download'


def test_quarantine_tolerates_a_missing_file(tmp_path):
    local_file = tmp_path / 'gone.grib'
    bt = _task_with_local_file(local_file)

    bt._quarantine_local_file(str(local_file), 'EOFError')

    assert not (tmp_path / 'gone.grib.corrupt').exists()
    assert not bt.data_index.loc[0, 'local_file_exists']
