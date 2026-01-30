from .functions import CaptureNewVariables, running_in_docker, DownloadMonitor, UploadMonitor
from .base import BaseTask
from .era5 import ERA5_ZAMBEZI_T2M, ERA5_ZAMBEZI_TP
from .irm import ALARO40L_CR, ALARO40L_T2M, ALARO40L_TP
from .gfs import GFS_025_T2M_CAUCASUS

__all__ = ['CaptureNewVariables', 'running_in_docker', 'DownloadMonitor', 'UploadMonitor',
           'BaseTask',
           'ERA5_ZAMBEZI_T2M', 'ERA5_ZAMBEZI_TP',
           'ALARO40L_CR', 'ALARO40L_T2M', 'ALARO40L_TP',
           'GFS_025_T2M_CAUCASUS',
           ]
