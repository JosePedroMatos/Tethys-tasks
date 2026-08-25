import pandas as pd

from tethys_tasks.functions import CompletenessIndex


def test_include_keeps_entries_from_earlier_runs(tmp_path):
    # The ERA5M regression: many storage files share one folder, and each run only sees the
    # year(s) in its own window. include() must not drop the rest.
    first = CompletenessIndex(tmp_path)
    (tmp_path / 'tethys_era5m_t2m_1950.nct').touch()
    (tmp_path / 'tethys_era5m_t2m_1951.nct').touch()
    first.include(['tethys_era5m_t2m_1950.nct'])
    first.write()

    second = CompletenessIndex(tmp_path)
    second.include(['tethys_era5m_t2m_1951.nct'])
    second.write()

    assert sorted(CompletenessIndex(tmp_path).get_complete()) == [
        'tethys_era5m_t2m_1950.nct', 'tethys_era5m_t2m_1951.nct']


def test_include_survives_a_run_whose_only_file_is_incomplete(tmp_path):
    # A run covering just the current (still filling) year must not wipe the sidecar.
    (tmp_path / 'tethys_era5m_t2m_2025.nct').touch()
    (tmp_path / 'tethys_era5m_t2m_2026.nct').touch()
    ci = CompletenessIndex(tmp_path)
    ci.include(['tethys_era5m_t2m_2025.nct'])
    ci.write()
    assert (tmp_path / 'completeness.csv').exists()

    current = CompletenessIndex(tmp_path)
    current.include([])
    current.remove(['tethys_era5m_t2m_2026.nct'])
    current.write()

    assert (tmp_path / 'completeness.csv').exists()
    assert CompletenessIndex(tmp_path).get_complete() == ['tethys_era5m_t2m_2025.nct']


def test_remove_still_clears_a_file(tmp_path):
    (tmp_path / 'a.nct').touch()
    ci = CompletenessIndex(tmp_path)
    ci.include(['a.nct'])
    ci.remove(['a.nct'])
    ci.write()

    assert CompletenessIndex(tmp_path).get_complete() == []


def test_missing_file_is_dropped_on_reload(tmp_path):
    (tmp_path / 'a.nct').touch()
    (tmp_path / 'b.nct').touch()
    ci = CompletenessIndex(tmp_path)
    ci.include(['a.nct', 'b.nct'])
    ci.write()

    (tmp_path / 'b.nct').unlink()
    reloaded = CompletenessIndex(tmp_path)
    reloaded.write()

    assert CompletenessIndex(tmp_path).get_complete() == ['a.nct']
