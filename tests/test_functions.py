import pytest
from tethys_tasks import CaptureNewVariables

def test_capture_new_variables():
    with CaptureNewVariables() as cnv:
        AZURE_CONTAINER = None
        CLEANUP_WINDOW = 'test_value'
    
    assert 'AZURE_CONTAINER' in cnv.new_vars
    assert 'CLEANUP_WINDOW' in cnv.new_vars
    assert cnv.new_vars['AZURE_CONTAINER'] is None