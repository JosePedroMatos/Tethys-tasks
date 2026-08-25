# AGENTS.md

Guidance for AI agents (and humans) working in this repository.

## What this is

`Tethys-tasks` acquires, processes, and stores meteorological raster data
(reanalysis and forecasts) from several sources. Every product is a class that
subclasses `BaseTask` (in [tethys_tasks/base.py](tethys_tasks/base.py)):

- `ERA5` / `ERA5W` / `ERA5M`, `C3S_*`, `GFS_025`, `GPM_IMERG_*`, `ICON_CH*`, `ICON_EU`,
  `CLMS_*`, `ECMWF_ENS`/`ECMWF_HRES`, `IRM`, MeteoFrance, etc.
- `RADCLIM_TP` / `QPE_TP` / `BESTQPE2_TP` (in
  [tethys_tasks/irm_radar.py](tethys_tasks/irm_radar.py)) are the exception to "acquires": the IRM
  radar archives are read **in place** from a local `origin_folder` (no download, no local copy, no
  cloud/Dropbox tier, no crop), so every write path into the origin is disabled there.
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
- `ICON_CH1_EPS_*` downloads ~207 MB of static model constants (the horizontal and
  vertical constants GRIBs, too large to keep in the repository) into
  `<LOCAL_FILE_FOLDER>/ICON_CH_CONSTANTS/` on first use, and reuses them afterwards.
  That folder is a mounted volume, so a Docker run only pays for it once.

## GRIB definitions (eccodes) -- read before touching any GRIB driver

eccodes caches its definitions on **first use**, so whichever set is used first wins for the
rest of the process. `codes_set_definitions_path()` afterwards is a no-op.

- Every driver except ICON-CH decodes with the stock eccodes definitions.
- ICON-CH needs the DWD/COSMO tables. `meteodatalab.data_source` applies them per call via
  `cosmo_grib_defs()` (from the version-matched `eccodes-cosmo-resources-python`) and restores
  the path afterwards. Do **not** set `ECCODES_DEFINITION_PATH`: `meteodatalab.grib_def_ctx`
  disables its own COSMO handling when that variable is set, and the overlay then leaks into
  every other driver. That is what broke all GRIB drivers on 2026-08-24, when a rebuild moved
  eccodes to 2.47 while `icon_ch.py` was still pointing at a vendored ~2.38 overlay.
- Consequence: **one class per process**. Each Airflow task runs one class in its own
  container, so ICON-CH is the only GRIB consumer in its process and COSMO is active from the
  first read. Reading any other GRIB first makes `grib_decoder.load` return `{}` for CLON/CLAT
  instead of raising -- `icon_ch.read_local` turns that into an explicit error.
  `acquisition_status(refresh=True)` is safe to loop over all classes: it never decodes GRIB.
- The GRIB stack is pinned as one family in `environment.yml`. `python -m
  tethys_tasks.check_grib_stack` verifies it (stock and COSMO, one subprocess each) and runs as
  a `Dockerfile` build step, so an incoherent image fails the build instead of the DAGs.

## Grid geometry (eckit-geo)

`earthkit-data` sets `ECCODES_ECKIT_GEO=1` at import (`earthkit/data/__init__.py`), and since
`icon_ch` imports it, **every** driver decodes grids through `eckit::geo`. Leave it that way:
eckit-geo is what supports unstructured grids, so turning it off breaks ICON-CH and
`ICON_WORLD` (`GribGeographyBuilder: cannot use unstructured grid`). `check_grib_stack` guards
against it being set to `0`.

The backends only disagree where a GRIB header is self-inconsistent. IRM/ALARO declares
`jDirectionIncrementInDegrees` 0.035 while `first/last/Nj` imply 0.035375; the legacy code put
the whole discrepancy in one trailing latitude row (which is the row `irm.py` drops with
`[:-1]`), eckit-geo spreads it across every row. That silently shifted the ALARO grid on
2026-08-24 and broke `join` against the archive with `Latitudes do not match`. `irm.py` now
rebuilds latitudes from the declared increment (`GRIB_latitudeOfFirstGridPointInDegrees` +
`arange * GRIB_jDirectionIncrementInDegrees`), so it no longer depends on the backend. Any new
driver reading a GRIB with an inconsistent header needs the same treatment -- the failure is
silent until a join against older stored data catches it. `python -m
tethys_tasks.check_grid_headers [CLASS ...]` flags such files from the headers alone (no stored
file needed). As of 2026-08-24 ALARO is the only product that trips it; ERA5/ERA5W/ERA5M, C3S,
ECMWF HRES/ENS, AROME, IPMA and ICON-CH are all exact, and GFS/GPM/CLMS are not GRIB.

## Corrupt local files

`BaseTask.store()` quarantines a local file it cannot read (GRIB decode error, broken archive,
or a leadtime mismatch in the strict join): it renames it to `<name>.corrupt`, drops it from the
folder's `completeness.csv`, stores whatever else was readable, and then **still raises** so the
Airflow retry re-downloads it. Nothing is ever deleted. It does not catch a file that decodes
*partially* and is read first in its storage group -- that still gets NaN-padded and stored, as
before. `gpm` has its own `_read_local_safe`; `irm_radar` must never quarantine, since its
"local" files are the read-only IRM archive.

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
- An accumulation or averaging period is labelled by its **start**: the data at
  `production_datetime + leadtime` covers the period *beginning* there. ERA5 hourly `tp`
  shifts its timestamps back one step for this, ERA5M puts a monthly total on the 1st, and
  C3S monthly means use 0-based leadtimes (CDS `leadtime_month=1` is the initialisation
  month itself, and its GRIB `valid_time` is the *end* of the averaged month).
