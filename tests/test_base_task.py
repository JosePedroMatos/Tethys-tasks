import pytest
import pandas as pd
from tethys_tasks import BaseTask

def test_basetask_init_default():
    bt = BaseTask()
    assert hasattr(bt, '_azure_container')
    assert hasattr(bt, '_cleanup_window')
    assert hasattr(bt, '_transfer_folder')

def test_basetask_kwargs_override():
    bt = BaseTask(azure_container='https://example.com', Cleanup_window=pd.DateOffset(months=1))
    assert bt._azure_container == 'https://example.com'
    assert bt._cleanup_window == pd.DateOffset(months=1)

def test_basetask_custom_kwargs():
    # breakpoint()
    bt = BaseTask(custom_param='test')
    assert hasattr(bt, 'custom_param')
    assert bt.custom_param == 'test'

    bt = BaseTask(Custom_Param='test')
    assert bt.Custom_Param == 'test'

@pytest.mark.parametrize('payload', [
    b'\x89HDF\r\n\x1a\n' + b'\x00' * 512,   # HDF5 magic, body truncated mid-write
    b'\x00' * 512,                          # zero-filled
    b'not a netcdf file at all',
])
def test_load_stored_file_returns_none_for_a_corrupt_file(tmp_path, capsys, payload):
    corrupt = tmp_path / 'tethys_tp_20260805.nct'
    corrupt.write_bytes(payload)

    assert BaseTask()._load_stored_file(corrupt) is None
    assert 'unreadable' in capsys.readouterr().out

def test_discard_unreadable_stored_file_marks_it_missing():
    bt = BaseTask()
    bt.data_index = pd.DataFrame({
        'stored_file_exists': [True, True],
        'stored_file_complete': [True, True],
        'data_exists': [True, True],
    })

    bt._discard_unreadable_stored_file('tethys_tp_20260805.nct', bt.data_index.index)

    assert not bt.data_index.any().any()