from __future__ import annotations
from pathlib import Path
import time
import threading
from collections.abc import Iterable
import sys
import importlib.resources as importlib_resources
import pandas as pd


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
    
class CompletenessIndex():

    def __init__(self, folder:Path):
        self.folder = folder
        self.index_file = folder / 'completeness.csv'

        self.folder.mkdir(exist_ok=True, parents=True)
        
        self.index = pd.Series([], index=pd.Index([], name='file_name'), name='complete', dtype=float)

        self.read()
        self.check_existance()
    
    def read(self):
        if self.index_file.exists():
            self.index = pd.read_csv(self.index_file, sep=',', index_col='file_name')

            if isinstance(self.index, pd.DataFrame):
                if self.index.shape[1]==1:
                    self.index = self.index.iloc[:, 0]
                else:
                    self.index = pd.Series([], index=pd.Index([], name='file_name'), name='complete')
                    return

            if not isinstance(self.index, pd.Series) or self.index.name != 'complete' or self.index.index.name != 'file_name':
                self.index = pd.Series([], index=pd.Index([], name='file_name'), name='complete')

    def check_existance(self):
        for f0 in self.index.index:
            if not (self.folder / f0).exists():
                self.index[f0] = False

    def write(self):
        self.index = self.index[self.index==True]
        try:
            if self.index.empty:
                if self.index_file.exists():
                    self.index_file.unlink()
            else:
                self.index.to_csv(self.index_file, sep=',')
        except Exception as ex:
            print(f'Update of completeness index failed: {self.index_file.parent.absolute()} ({ex}).')
            

    def remove(self, files:Iterable):
        for f0 in files:
            if f0 in self.index.index:
                self.index[f0] = False

    def include(self, files:Iterable):
        self.index = self.index.reindex(files, fill_value=True)

    def get_complete(self):
        return self.index[self.index].index.tolist()

class _VariableCapture:
    def __init__(self, new_vars: dict):
        self.new_vars = new_vars


def create_kml_classes(base_class, variable_kwargs:dict={}) -> None:
    '''
    Docstring for create_kml_classes
    
    Creates regional classes based on a base class and a list of parameters at runtime.
    Does so for each .kml file in resources.

    :param base_class: The class that serves as the basis for the regional classes. Must be defined in the respective .py file. Example: ERA5.
    :param variable_kwargs: a dict of lists of variables. Must be linked to the base class. Example: {"VARIABLE": ["tp", "t2m"]}
    '''
    try:
        resources = importlib_resources.files('tethys_tasks.resources')
        kml_entries = [entry for entry in resources.iterdir() if entry.name.lower().endswith('.kml')]
    except Exception:
        resources_path = Path(__file__).resolve().parent / 'resources'
        kml_entries = list(resources_path.glob('*.kml')) if resources_path.exists() else []

    target_module = sys.modules.get(base_class.__module__)
    if target_module is None:
        raise RuntimeError(f'Base class module not loaded: {base_class.__module__}')

    print(f'')

    keys = variable_kwargs.keys()
    sizes = list(set([len(variable_kwargs[k]) for k in keys]))
    if len(sizes)>1:
        raise Exception(f'All variables called in "create_kml_classes" must have a list of paramters of the same length ({base_class.__name__}).')

    for entry in sorted(kml_entries, key=lambda p: p.name):
        zone = Path(entry.name).stem.lower()
        kml = f'tethys_tasks/resources/{entry.name}'

        if len(sizes)==0:
            loop = [0]
        else:
            loop = [i for i in range(sizes[0])]
        for i0 in loop:
            if 'VARIABLE' in keys:
                class_name = f'{base_class.__name__}_{variable_kwargs["VARIABLE"][i0].upper()}_{zone.upper()}'
            else:
                class_name = f'{base_class.__name__}_{zone.upper()}'
                
            if hasattr(target_module, class_name):
                continue

            if len(sizes)>0:
                _variable_kwargs = {k: i[i0] for k, i in variable_kwargs.items()}
            else:
                _variable_kwargs = {}

            attrs = {
                '__module__': base_class.__module__,
                f'_{class_name}_VARIABLES': _VariableCapture(
                    {
                        'SOURCE_KML': kml,
                        'STORAGE_KML': kml,
                        'ZONE': zone,
                        **_variable_kwargs
                    }
                ),
                'SOURCE_KML': kml,
                'STORAGE_KML': kml,
                'ZONE': zone,
                **_variable_kwargs
            }

            setattr(target_module, class_name, type(class_name, (base_class,), attrs))
            if not target_module.__name__.startswith('__main__'):
                print(f'    Class "{target_module.__name__}.{class_name}" created at runtime.')
            