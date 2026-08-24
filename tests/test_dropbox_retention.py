'''
Dropbox retention of stored files.

Retention must depend on what is on Dropbox, not on the run's date_from window.
A short window (GFS uses now-2d against weekly storage files) leaves a single
storage file in data_index; ranking against the index alone marked every older
Dropbox file stale and deleted it, so weekly products kept only one file.
'''

import pandas as pd
import pytest

from tethys_tasks import BaseTask
from tethys_tasks import base as base_module
from tethys_tasks.base import STORED_RETENTION_COUNT

# Mirrors the production root. common_dropbox_root() stats the path, so this must
# not name a real local directory.
DROPBOX_ROOT = '/01.WorkInProgress/Tethys/tethys_tasks_stored'
GFS_TEMPLATE = 'NOAA_GFS_0.25/gfs_tmp_zambezi/{floor_year}/tethys_NOAA_GFS_0.25_{floor_7_days}.nct'
GFS_FOLDER = DROPBOX_ROOT + '/NOAA_GFS_0.25/gfs_tmp_zambezi'


def _remote(path):
    return {
        'name': path.rsplit('/', 1)[-1],
        'path_display': path,
        'path_lower': path.lower(),
        'id': 'id:' + path,
        'size': 1,
        'content_hash': 'remote-hash',
        'server_modified': None,
    }


@pytest.fixture
def sync(monkeypatch, tmp_path):
    '''
    Drives _sync_latest_stored_upload against a fake Dropbox.

    Returns a callable taking the weeks present on Dropbox and the weeks present
    in data_index (i.e. covered by the run's date_from window), and reporting
    which paths were deleted and uploaded.
    '''

    def run(remote_weeks, index_weeks, extra_remote=(), template=GFS_TEMPLATE):
        deleted = []
        uploaded = []

        def stored_path(week):
            local = tmp_path / f'tethys_NOAA_GFS_0.25_{week}.nct'
            local.write_bytes(b'x')
            return str(local)

        def dropbox_path(week):
            return f'{GFS_FOLDER}/{week[:4]}/tethys_NOAA_GFS_0.25_{week}.nct'

        remote_files = {p.lower(): _remote(p) for p in
                        [dropbox_path(w) for w in remote_weeks] + list(extra_remote)}

        task = BaseTask()
        task._sync_latest_stored = True
        task._dropbox_root_path = DROPBOX_ROOT
        task._storage_path_template = template
        task.verbose = 0

        task.data_index = pd.DataFrame({
            'dropbox_file': [dropbox_path(w) for w in index_weeks],
            'stored_file': [stored_path(w) for w in index_weeks],
            'stored_file_complete': True,
            'production_datetime': [pd.Timestamp(w.replace('.', '-')) for w in index_weeks],
            'stored_file_exists': True,
        })

        monkeypatch.setattr(task, '_update_index_and_completeness', lambda **kw: None)
        monkeypatch.setattr(task, '_get_dropbox_connection', lambda: object())
        # Honour the requested root, so a listing that reaches outside the task's
        # own folder shows up as a test failure rather than passing silently.
        monkeypatch.setattr(base_module, 'list_dropbox_files', lambda client, root: {
            k: v for k, v in remote_files.items() if k.startswith(root.lower() + '/')
        })
        # Local content never matches the remote hash, so every desired file uploads.
        monkeypatch.setattr(base_module, 'compare_local_to_remote_hash', lambda *a: False)
        monkeypatch.setattr(base_module, 'upload_file',
                            lambda client, local, remote: uploaded.append(remote))
        monkeypatch.setattr(base_module, 'delete_dropbox_paths',
                            lambda client, paths: deleted.extend(paths) or list(paths))

        task._sync_latest_stored_upload()
        return {'deleted': sorted(deleted), 'uploaded': sorted(uploaded)}

    return run


def test_short_window_keeps_older_dropbox_files(sync):
    '''The regression: a 2-day window sees one week, Dropbox holds five.'''

    result = sync(
        remote_weeks=['2026.07.20', '2026.07.27', '2026.08.03', '2026.08.10', '2026.08.17'],
        index_weeks=['2026.08.17'],
    )

    assert result['deleted'] == [
        f'{GFS_FOLDER}/2026/tethys_NOAA_GFS_0.25_2026.07.20.nct',
        f'{GFS_FOLDER}/2026/tethys_NOAA_GFS_0.25_2026.07.27.nct',
    ]
    # The two weeks preceding the current one survive: three files remain.
    assert f'{GFS_FOLDER}/2026/tethys_NOAA_GFS_0.25_2026.08.10.nct' not in result['deleted']
    assert f'{GFS_FOLDER}/2026/tethys_NOAA_GFS_0.25_2026.08.03.nct' not in result['deleted']


def test_retention_count_is_respected(sync):
    remote_weeks = ['2026.07.20', '2026.07.27', '2026.08.03', '2026.08.10', '2026.08.17']
    result = sync(remote_weeks=remote_weeks, index_weeks=['2026.08.17'])

    assert len(remote_weeks) - len(result['deleted']) == STORED_RETENTION_COUNT


def test_current_file_is_still_uploaded(sync):
    result = sync(
        remote_weeks=['2026.08.03', '2026.08.10', '2026.08.17'],
        index_weeks=['2026.08.17'],
    )

    assert result['uploaded'] == [f'{GFS_FOLDER}/2026/tethys_NOAA_GFS_0.25_2026.08.17.nct']


def test_retention_spans_the_year_boundary(sync):
    '''Year folders must rank chronologically, not restart the count.'''

    result = sync(
        remote_weeks=['2025.12.22', '2025.12.29', '2026.01.05'],
        index_weeks=['2026.01.05'],
    )

    assert result['deleted'] == []


def test_older_year_is_pruned_once_three_newer_exist(sync):
    result = sync(
        remote_weeks=['2025.12.22', '2025.12.29', '2026.01.05', '2026.01.12'],
        index_weeks=['2026.01.12'],
    )

    assert result['deleted'] == [f'{GFS_FOLDER}/2025/tethys_NOAA_GFS_0.25_2025.12.22.nct']


def test_foreign_files_are_never_deleted(sync):
    '''Anything the task does not itself produce is out of scope.'''

    foreign = [
        f'{GFS_FOLDER}/2026/notes.txt',
        f'{GFS_FOLDER}/2026/tethys_imerg_late_2026.01.05.nct',
        f'{GFS_FOLDER}/readme.md',
    ]
    result = sync(
        remote_weeks=['2026.07.20', '2026.07.27', '2026.08.03', '2026.08.10', '2026.08.17'],
        index_weeks=['2026.08.17'],
        extra_remote=foreign,
    )

    assert not set(foreign) & set(result['deleted'])


def test_other_tasks_folders_are_out_of_listing_scope(sync):
    '''A sibling product under the same Dropbox root must not be touched.'''

    sibling = [
        f'{DROPBOX_ROOT}/NOAA_GFS_0.25/gfs_prate_zambezi/2026/tethys_NOAA_GFS_0.25_2026.07.20.nct',
        f'{DROPBOX_ROOT}/ERA5_T2M/era5_t2m_zambezi/2026/tethys_era5_t2m_2026.01.01.nct',
    ]
    result = sync(
        remote_weeks=['2026.07.20', '2026.07.27', '2026.08.03', '2026.08.10', '2026.08.17'],
        index_weeks=['2026.08.17'],
        extra_remote=sibling,
    )

    assert not set(sibling) & set(result['deleted'])


def test_desired_files_survive_even_when_older_than_the_newest_three(sync):
    '''A backfill re-uploading an old week must not delete what it just wrote.'''

    result = sync(
        remote_weeks=['2026.07.27', '2026.08.03', '2026.08.10', '2026.08.17'],
        index_weeks=['2026.07.27'],
    )

    target = f'{GFS_FOLDER}/2026/tethys_NOAA_GFS_0.25_2026.07.27.nct'
    assert target in result['uploaded']
    assert target not in result['deleted']


def test_pruning_is_skipped_without_a_static_prefix(sync):
    '''No task-specific folder to scope to -> delete nothing rather than guess.'''

    result = sync(
        remote_weeks=['2026.07.20', '2026.08.17'],
        index_weeks=['2026.08.17'],
        template='{floor_year}/tethys_NOAA_GFS_0.25_{floor_7_days}.nct',
    )

    assert result['deleted'] == []


def test_retention_scope_is_task_specific():
    task = BaseTask()
    task._dropbox_root_path = DROPBOX_ROOT
    task._storage_path_template = GFS_TEMPLATE

    root, pattern = task._stored_retention_scope()

    assert root == GFS_FOLDER
    assert pattern.match('tethys_NOAA_GFS_0.25_2026.08.17.nct')
    assert not pattern.match('tethys_era5_t2m_2026.08.01.nct')
