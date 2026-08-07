import pytest
from tethys_tasks import CaptureNewVariables, CompletenessIndex

def test_capture_new_variables():
    with CaptureNewVariables() as cnv:
        AZURE_CONTAINER = None
        CLEANUP_WINDOW = 'test_value'

    assert 'AZURE_CONTAINER' in cnv.new_vars
    assert 'CLEANUP_WINDOW' in cnv.new_vars
    assert cnv.new_vars['AZURE_CONTAINER'] is None

@pytest.mark.parametrize('payload', [
    b'\x00' * 194,              # zero-filled by an unclean shutdown mid-write
    b'',                        # empty
    b'file_na',                 # truncated header
    b'\xff\xfe not a csv',      # undecodable
])
def test_completeness_index_survives_a_corrupt_file(tmp_path, payload):
    (tmp_path / 'completeness.csv').write_bytes(payload)

    ci = CompletenessIndex(tmp_path)

    assert ci.get_complete() == []

def test_completeness_index_rewrites_a_corrupt_file(tmp_path):
    (tmp_path / 'completeness.csv').write_bytes(b'\x00' * 194)
    (tmp_path / 'data.zip').write_bytes(b'x')

    ci = CompletenessIndex(tmp_path)
    ci.include(['data.zip'])
    ci.write()

    assert CompletenessIndex(tmp_path).get_complete() == ['data.zip']
