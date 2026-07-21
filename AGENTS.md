# AGENTS.md

Guidance for AI agents (and humans) working in this repository.

## What this is

`Tethys-tasks` acquires, processes, and stores meteorological raster data
(reanalysis and forecasts) from several sources. Every product is a class that
subclasses `BaseTask` (in [tethys_tasks/base.py](tethys_tasks/base.py)):

- `ERA5` / `ERA5W`, `C3S_*`, `GFS_025`, `GPM_IMERG_*`, `ICON_CH*`, `ICON_EU`,
  `CLMS_*`, `ECMWF_ENS`/`ECMWF_HRES`, `IRM`, MeteoFrance, etc.
- Region-specific classes (e.g. `ERA5_ZAMBEZI_T2M`, `GFS_025_PRATE_BELGIUM`) are
  generated at import time from the `.kml` files in `tethys_tasks/resources/` by
  `create_kml_classes` (in [tethys_tasks/functions.py](tethys_tasks/functions.py)).

## Core model

- Class-level config vars (captured via `CaptureNewVariables`) become instance
  attributes with a lowercase `_` prefix, overridable via `__init__` kwargs
  (e.g. `LEADTIMES` → `self._leadtimes`, `date_from` kwarg → `self._date_from`).
- State lives in the in-memory DataFrame `self.data_index`, built by `populate()`
  in `__init__`. It has one row per `(production_datetime, leadtime)` and boolean
  columns `cloud_file_exists`, `local_file_exists`, `stored_file_exists`,
  `data_exists`, `local_file_complete`, `stored_file_complete`.
- Three storage tiers, each rooted in an env var: local (`LOCAL_FILE_FOLDER`),
  storage/`.nct` (`STORAGE_FILE_FOLDER`), Azure cloud (`CLOUD_STORAGE_FOLDER`);
  paths are produced from the `*_PATH_TEMPLATE` / `CLOUD_TEMPLATE` strftime
  templates. There is no database — the folder layout plus per-folder
  `completeness.csv` sidecars are the record.
- Pipeline entry points: `retrieve()`, `store()`, `update()` (full pipeline),
  and helpers like `retrieve_and_upload()`. `main.py` instantiates a class by
  name and calls a method by name (used by the Docker CLI).

## Environment / running

- Conda env `tethys_tasks` (see `environment.yml`); it depends on the external
  `meteoraster` package, so a bare system Python will fail to import.
- Run a task method (outside Docker):
  ```bash
  python main.py <CLASS> <method> --class_kwargs "{\"date_from\": \"'2025-10-01'\"}" --fun_kwargs "{}"
  ```
- Or via Docker (see [README.md](README.md) for the full examples).

## Acquisition status (reporting)

`BaseTask.acquisition_status(refresh=False)` is a read-only instance method that
summarizes acquisition health from `self.data_index`. It returns a dict:

```python
{'last_acquisition': pd.Timestamp | None,   # last production_datetime with any leadtime hit
 'success_rate':     float | None,          # hit_leadtimes / total_leadtimes at that date (0..1)
 'hit_leadtimes':    int,
 'total_leadtimes':  int}
```

- `last_acquisition` = most recent `production_datetime` with **any** leadtime hit
  (`data_exists` True on any of its rows).
- `success_rate` = fraction of leadtimes hit **at that last date** (the "hit
  leadtimes" ratio, most meaningful for many-leadtime forecasts like `GFS_025`).
- `refresh=False` (default): report from the current in-memory index — call it
  after a `retrieve()`/`update()` in the same process.
- `refresh=True`: first rebuild the index from **stored + local** files
  (`cloud=False`), which is fast and **network-free** (uses `completeness.csv`).
  Use this for a standalone report (e.g. an Airflow report DAG).

### Docker CLI (`main.py` prints `Result: {...}`)
```bash
docker-compose run --rm tethys-tasks ICON_CH2_EPS_TOT_PREC acquisition_status --class_kwargs "{\"date_from\": \"'2026-04-07 12:00:00'\"}" --fun_kwargs "{\"refresh\": true}"
```

### Python (e.g. a daily Airflow report DAG)
```python
import tethys_tasks

report = {
    name: cls(verbose=0).acquisition_status(refresh=True)
    for name in tethys_tasks.__all__
    if isinstance((cls := getattr(tethys_tasks, name)), type)
    and issubclass(cls, tethys_tasks.BaseTask)
    and cls is not tethys_tasks.BaseTask
}
```

## Conventions

- Match the surrounding style (this codebase uses single-quoted strings, the
  `CaptureNewVariables` config pattern, and pandas-heavy vectorized code).
- New behavior on all products belongs on `BaseTask`; product-specific behavior
  is overridden in the subclass (`_download_from_source`, `read_local`,
  `complete_local_files`, `populate`, `store`, ...).
- Keep report/status helpers read-only and network-free by default.
