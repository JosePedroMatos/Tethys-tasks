from __future__ import annotations

from hashlib import sha256
import posixpath
from pathlib import Path
from typing import Iterable

import dropbox as dropbox_sdk
from dropbox.exceptions import ApiError
from dropbox.files import DeleteArg, FileMetadata, FolderMetadata, UploadSessionCursor, WriteMode


DROPBOX_BLOCK_SIZE = 4 * 1024 * 1024
DROPBOX_SIMPLE_UPLOAD_LIMIT = 150 * 1024 * 1024


def _is_not_found_api_error(exc: ApiError) -> bool:
	error = getattr(exc, 'error', None)
	if error is None or not hasattr(error, 'is_path') or not error.is_path():
		return False
	lookup_error = error.get_path()
	return hasattr(lookup_error, 'is_not_found') and lookup_error.is_not_found()

def normalize_dropbox_path(path: str) -> str:
	normalized = str(path).replace('\\', '/').strip()
	if not normalized:
		return '/'
	if not normalized.startswith('/'):
		normalized = '/' + normalized
	while '//' in normalized:
		normalized = normalized.replace('//', '/')
	if len(normalized) > 1 and normalized.endswith('/'):
		normalized = normalized[:-1]
	return normalized

def common_dropbox_root(paths: Iterable[str], fallback: str = '/') -> str:
	normalized_paths = [normalize_dropbox_path(path) for path in paths if str(path).strip()]
	common_path = posixpath.commonpath(normalized_paths)
	if Path(common_path).is_file() or common_path.endswith('.nct'):
		common_path = Path(common_path).parent
	return normalize_dropbox_path(common_path)

def local_path_to_dropbox_path(local_path: str | Path, local_root: str | Path, dropbox_root: str) -> str:
	local_path_str = str(local_path).replace('\\', '/').strip()
	local_root_str = str(local_root).replace('\\', '/').strip().rstrip('/')

	relative_path = local_path_str[len(local_root_str) + 1:]

	return dropbox_root + '/' + relative_path

def get_dropbox_client(
	*,
	refresh_token: str = '',
	app_key: str = '',
	app_secret: str = '',
	access_token: str = '',
	timeout: float = 100,
) -> dropbox_sdk.Dropbox:
	refresh_token = (refresh_token or '').strip()
	app_key = (app_key or '').strip()
	app_secret = (app_secret or '').strip()
	access_token = (access_token or '').strip()

	if refresh_token:
		if not app_key:
			raise ValueError('DROPBOX_APP_KEY is required when using DROPBOX_REFRESH_TOKEN.')
		return dropbox_sdk.Dropbox(
			oauth2_refresh_token=refresh_token,
			app_key=app_key,
			app_secret=app_secret or None,
			timeout=timeout,
		)

	if access_token:
		return dropbox_sdk.Dropbox(oauth2_access_token=access_token, timeout=timeout)

	raise ValueError('Dropbox credentials are missing. Provide DROPBOX_REFRESH_TOKEN or DROPBOX_ACCESS_TOKEN.')

def compute_dropbox_content_hash(file_path: str | Path, block_size: int = DROPBOX_BLOCK_SIZE) -> str:
	digest = sha256()
	with Path(file_path).open('rb') as handle:
		while True:
			chunk = handle.read(block_size)
			if not chunk:
				break
			digest.update(sha256(chunk).digest())
	return digest.hexdigest()

def list_dropbox_files(
	client: dropbox_sdk.Dropbox,
	root: str,
	recursive: bool = True,
) -> dict[str, dict]:
	try:
		result = client.files_list_folder(root, recursive=recursive)
	except ApiError as exc:
		if _is_not_found_api_error(exc):
			return {}
		raise
	files = {}

	while True:
		for entry in result.entries:
			if isinstance(entry, FileMetadata):
				files[entry.path_lower] = {
					'name': entry.name,
					'path_display': entry.path_display,
					'path_lower': entry.path_lower,
					'id': entry.id,
					'size': entry.size,
					'content_hash': entry.content_hash,
					'server_modified': entry.server_modified,
				}
		if not result.has_more:
			break
		result = client.files_list_folder_continue(result.cursor)

	return files

def list_dropbox_folders(
	client: dropbox_sdk.Dropbox,
	root: str,
	recursive: bool = True,
) -> dict[str, dict]:
	root = normalize_dropbox_path(root)
	try:
		result = client.files_list_folder(root, recursive=recursive)
	except ApiError as exc:
		if _is_not_found_api_error(exc):
			return {}
		raise
	folders = {}

	while True:
		for entry in result.entries:
			if isinstance(entry, FolderMetadata):
				folders[entry.path_lower] = {
					'name': entry.name,
					'path_display': entry.path_display,
					'path_lower': entry.path_lower,
					'id': entry.id,
				}
		if not result.has_more:
			break
		result = client.files_list_folder_continue(result.cursor)

	return folders

def ensure_dropbox_folder(client: dropbox_sdk.Dropbox, folder_path: str) -> None:
	folder_path = normalize_dropbox_path(folder_path)
	if folder_path == '/':
		return

	parts = [part for part in folder_path.split('/') if part]
	current = ''
	for part in parts:
		current += '/' + part
		try:
			client.files_create_folder_v2(current)
		except ApiError as exc:
			error = getattr(exc, 'error', None)
			if error is not None and error.is_path() and error.get_path().is_conflict():
				continue
			raise

def upload_file(
	client: dropbox_sdk.Dropbox,
	local_path: str | Path,
	remote_path: str,
	overwrite: bool = True,
	chunk_size: int = 8 * 1024 * 1024,
) -> dict:
	path = Path(local_path)
	remote_path = normalize_dropbox_path(remote_path)
	ensure_dropbox_folder(client, str(Path(remote_path).parent).replace('\\', '/'))

	mode = WriteMode.overwrite if overwrite else WriteMode.add
	file_size = path.stat().st_size

	with path.open('rb') as handle:
		if file_size <= DROPBOX_SIMPLE_UPLOAD_LIMIT:
			metadata = client.files_upload(handle.read(), remote_path, mode=mode)
			return {
				'path_display': metadata.path_display,
				'path_lower': metadata.path_lower,
				'size': metadata.size,
				'content_hash': metadata.content_hash,
			}

		session = client.files_upload_session_start(handle.read(chunk_size))
		cursor = UploadSessionCursor(session_id=session.session_id, offset=handle.tell())
		commit = dropbox_sdk.files.CommitInfo(path=remote_path, mode=mode)

		while handle.tell() < file_size:
			remaining = file_size - handle.tell()
			chunk = handle.read(min(chunk_size, remaining))
			if remaining <= chunk_size:
				metadata = client.files_upload_session_finish(chunk, cursor, commit)
				return {
					'path_display': metadata.path_display,
					'path_lower': metadata.path_lower,
					'size': metadata.size,
					'content_hash': metadata.content_hash,
				}

			client.files_upload_session_append_v2(chunk, cursor)
			cursor.offset = handle.tell()

	raise RuntimeError(f'Upload did not finish for {path}.')

def download_file(client: dropbox_sdk.Dropbox, remote_path: str, local_path: str | Path) -> dict:
	target = Path(local_path)
	target.parent.mkdir(parents=True, exist_ok=True)
	metadata = client.files_download_to_file(str(target), remote_path)
	return {
		'path_display': metadata.path_display,
		'path_lower': metadata.path_lower,
		'size': metadata.size,
		'content_hash': metadata.content_hash,
	}

def delete_dropbox_paths(client: dropbox_sdk.Dropbox, paths: Iterable[str]) -> list[str]:
	normalized = [path for path in paths]
	if not normalized:
		return []

	entries = [DeleteArg(path) for path in normalized]
	deleted = []

	for start in range(0, len(entries), 1000):
		batch = entries[start:start + 1000]
		launch = client.files_delete_batch(batch)
		if launch.is_complete():
			deleted.extend([entry.path for entry in batch])
			continue

		status = client.files_delete_batch_check(launch.get_async_job_id())
		while status.is_in_progress():
			status = client.files_delete_batch_check(launch.get_async_job_id())

		if status.is_complete():
			deleted.extend([entry.path for entry in batch])
			continue

		raise RuntimeError('Dropbox batch delete failed to complete.')

	return deleted

def compare_local_to_remote_hash(local_path: str | Path, remote_metadata: dict | None) -> bool:
	if remote_metadata is None:
		return False
	path = Path(local_path)
	if not path.exists() or not path.is_file():
		return False
	return compute_dropbox_content_hash(path) == remote_metadata.get('content_hash', '')