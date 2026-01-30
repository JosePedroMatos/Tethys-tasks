import pytest
import pandas as pd
from tasks import BaseTask

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