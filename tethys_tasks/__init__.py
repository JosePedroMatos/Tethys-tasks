from .functions import (
    CaptureNewVariables,
    running_in_docker,
    DownloadMonitor,
    UploadMonitor,
    CompletenessIndex,
    create_kml_classes,
)
from .base import BaseTask
from . import era5 as _era5
from . import gfs as _gfs
from . import irm as _irm
from . import c3s as _c3s
from . import gpm as _gpm
from . import clms_snow as _clms_snow
from . import icon_ch as _icon_ch
from . import meteofrance as _meteofrance

__all__ = [
    'CaptureNewVariables',
    'running_in_docker',
    'DownloadMonitor',
    'UploadMonitor',
    'BaseTask',
    'CompletenessIndex',
    'create_kml_classes',
]

def _export_public_classes(module):
    public = [
        name for name, obj in vars(module).items()
        if isinstance(obj, type)
        and obj.__module__ == module.__name__
        and not name.startswith('_')
    ]
    for name in public:
        globals()[name] = getattr(module, name)
    __all__.extend(public)

for _module in (_era5, _irm, _gfs, _gpm, _c3s, _icon_ch, _clms_snow, _meteofrance):
    _export_public_classes(_module)
