from __future__ import annotations
from contextlib import ContextDecorator
import json
from pathlib import Path
import time
import threading

class CaptureNewVariables():
    '''
    This context is used so that all class variables are associated with local variables
        _ is added as a prefix
        all characters converted to lowercase

    The purpose is to allow overridding all class defaults as kwargs passed to the class when instantiated
    '''

    def __init__(self):
        self.locals_ini = None
    
    def __enter__(self):
        # Get the caller's local namespace
        import inspect
        frame = inspect.currentframe().f_back
        self.locals_ini = dict(frame.f_locals)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        import inspect
        frame = inspect.currentframe().f_back
        locals_end = frame.f_locals
        
        # Compare initial and final local variables
        self.new_vars = {key: value for key, value in locals_end.items() if key not in self.locals_ini}

        return None

def running_in_docker() -> bool:
    # Docker
    if Path("/.dockerenv").exists():
        return True

    # Podman (and some OCI setups)
    if Path("/run/.containerenv").exists():
        return True

    # Fallback: cgroup hints (works on many Linux hosts, less so on Docker Desktop)
    try:
        cgroup = Path("/proc/self/cgroup").read_text(errors="ignore")
        if any(x in cgroup for x in ("docker", "kubepods", "containerd")):
            return True
    except FileNotFoundError:
        pass

    return False

class DownloadMonitor:
    """Tracks aggregated download rate across multiple files."""

    def __init__(self):
        self._start = None
        self._lock = threading.Lock()
        self.downloaded_bytes = 0
        self.completed = 0

    def __enter__(self):
        self._start = time.perf_counter()
        return self

    def mark_success(self, local_path):
        p = Path(local_path)
        if p.exists():
            with self._lock:
                self.downloaded_bytes += p.stat().st_size
                self.completed += 1

        elapsed = max(time.perf_counter() - self._start, 1e-6)
        kb = self.downloaded_bytes / 1024
        rate = kb / elapsed
        return f'Downloaded {p.name} ({rate:.1f} KB/s).'

    def __exit__(self, exc_type, exc, tb):
        return None


class UploadMonitor:
    """Tracks aggregated upload rate across multiple files."""

    def __init__(self):
        self._start = None
        self._lock = threading.Lock()
        self.uploaded_bytes = 0
        self.completed = 0

    def __enter__(self):
        self._start = time.perf_counter()
        return self

    def mark_success(self, local_path, upload_path):
        p = Path(local_path)
        if p.exists():
            with self._lock:
                self.uploaded_bytes += p.stat().st_size
                self.completed += 1

        elapsed = max(time.perf_counter() - self._start, 1e-6)
        kb = self.uploaded_bytes / 1024
        rate = kb / elapsed
        return f'Uploaded {upload_path} ({rate:.1f} KB/s).'

    def __exit__(self, exc_type, exc, tb):
        return None