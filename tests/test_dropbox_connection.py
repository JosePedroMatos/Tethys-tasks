'''
Read-only Dropbox connectivity checks.

The live download tests build their tasks with sync_latest_stored=False so they
never touch the shared account (update() would otherwise sync and prune real
remote files). This module is the one place that verifies the Dropbox
credentials and root path actually work, and it only reads: no upload, no
delete.
'''

import os

import pytest
from dropbox.exceptions import ApiError

from tethys_tasks import BaseTask
from tethys_tasks.dropbox_sync import normalize_dropbox_path


def _has_credentials() -> bool:
    return bool(
        (os.getenv('DROPBOX_REFRESH_TOKEN') or '').strip()
        or (os.getenv('DROPBOX_ACCESS_TOKEN') or '').strip()
    )


requires_dropbox = pytest.mark.skipif(
    not _has_credentials(),
    reason='Set DROPBOX_REFRESH_TOKEN or DROPBOX_ACCESS_TOKEN to run the Dropbox connection checks.',
)


@pytest.fixture(scope='module')
def task():
    return BaseTask(verbose=0)


@requires_dropbox
def test_credentials_authenticate(task):
    # check_user is Dropbox's echo endpoint: it exercises the token without
    # reading or writing any file. _get_dropbox_connection converts an AuthError
    # into a RuntimeError carrying the token-setup hint.
    client = task._get_dropbox_connection()

    assert client.check_user('tethys-tasks').result == 'tethys-tasks'


@requires_dropbox
def test_configured_root_path_exists(task):
    root = normalize_dropbox_path(task._dropbox_root_path)
    if root == '/':
        pytest.skip('DROPBOX_ROOT_PATH is the account root, which has no metadata entry.')

    client = task._get_dropbox_connection()
    try:
        metadata = client.files_get_metadata(root)
    except ApiError as exc:
        pytest.fail('DROPBOX_ROOT_PATH %r is not reachable: %s' % (root, exc))

    assert metadata.path_lower == root.lower()


@requires_dropbox
def test_connection_is_cached(task):
    # base.py holds one client per task; the sync helpers re-request it per call.
    assert task._get_dropbox_connection() is task._get_dropbox_connection()
