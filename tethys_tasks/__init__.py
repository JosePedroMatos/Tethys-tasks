from .functions import (
    CaptureNewVariables,
    running_in_docker,
    DownloadMonitor,
    UploadMonitor,
    CompletenessIndex,
    create_kml_classes,
)
from .base import BaseTask
try:
    from . import era5 as _era5
except ImportError:
    _era5 = None
try:
    from . import gfs as _gfs
except ImportError:
    _gfs = None
try:
    from . import irm as _irm
except ImportError:
    _irm = None
try:
    from . import c3s as _c3s
except ImportError:
    _c3s = None
try:
    from . import gpm as _gpm
except ImportError:
    _gpm = None
try:
    from . import clms_snow as _clms_snow
except ImportError:
    _clms_snow = None
try:
    from . import icon_ch as _icon_ch
except ImportError:
    _icon_ch = None
try:
    from . import icon_eu as _icon_eu
except ImportError:
    _icon_eu = None
try:
    from . import meteofrance as _meteofrance
except ImportError:
    _meteofrance = None
try:
    from . import ecmwf_forecasts as _ecmwf
except ImportError:
    _ecmwf = None

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

for _module in (_era5, _irm, _gfs, _gpm, _c3s, _icon_ch, _icon_eu, _clms_snow, _meteofrance, _ecmwf):
    if _module is not None:
        _export_public_classes(_module)
