# Tethys-tasks
Code to run tasks associated with Tethys

## Conda installation (required)
This project is intended to be installed with Conda. Runtime dependencies are defined in environment.yml.

```bash
conda env create -f environment.yml
conda activate tethys_tasks
```

### Build a wheel (optional)
```bash
python -m build
```

## Docker Usage
This project is designed to be run within a Docker container, typically orchestrated by an external Airflow instance.

### Build the image
```bash
docker build -t tethys-tasks:latest .

docker save -o tethys-tasks.tar tethys-tasks
docker load -i tethys-tasks.tar
```

### Run a specific class function (built image)
Uses the code baked into the image.
```bash
docker-compose run --rm tethys-tasks C3S_ECMWF_TPRATE_IBERIA update --class_kwargs "{\"date_from\": \"'2025-12-01'\", \"download_from_origin\": \"True\"}" --fun_kwargs "{}"
docker-compose run --rm tethys-tasks ERA5_TP_CAUCASUS update --class_kwargs "{\"date_from\": \"'2025-05-01'\", \"download_from_origin=True\": \"True\"}"
docker-compose run --rm tethys-tasks ERA5_ZAMBEZI_TP update --class_kwargs "{\"date_from\": \"'2025-05-01'\", \"download_from_origin=False\": \"False\"}" 
docker-compose run --rm tethys-tasks  update --class_kwargs "{\"date_from\": \"'2026-04-01'\"}"
docker-compose run --rm tethys-tasks GPM_IMERG_LATE_TAJIKISTAN update --class_kwargs "{\"date_from\": \"'2026-04-01'\"}"
docker-compose run --rm tethys-tasks GPM_IMERG_LATE_CAUCASUS update --class_kwargs "{\"date_from\": \"'2026-04-01'\"}"
docker-compose run --rm tethys-tasks GPM_IMERG_LATE_ZAMBEZI update --class_kwargs "{\"date_from\": \"'2026-04-01'\"}"

docker compose run --rm tethys-tasks ERA5_SD_TAJIKISTAN update --class_kwargs "{\"download_from_origin\": \"True\", \"date_from\": \"'2026-01-01'\", \"era5_local_world\": \"True\", \"source_parallel_transfers\": 2}"
```

### Run a specific class function ("real-time code")
Uses local code instead of the image's code.
```bash
docker run --rm 
  -v "%cd%:/app" 
  -v "C:\tethys-tasks local test:/tmp/local_files" 
  -v "C:\tethys-tasks storage test:/tmp/storage_files" 
  --env-file .env 
  tethys-tasks:latest 
  ERA5_ZAMBEZI_T2M retrieve_and_upload 
  --class_args "[\"True\"]" --class_kwargs "{\"date_from\": \"'2025-10-01'\"}" --fun_kwargs "{}"


### Run outside docker
```bash
python main.py ERA5_ZAMBEZI_T2M retrieve --class_kwargs "{\"date_from\": \"'2025-10-01'\"}"
```

## Acquisition status (reporting)

`BaseTask.acquisition_status()` reports the date of the **last successful data
acquisition** and the **success rate** (fraction of leadtimes hit) at that date.
It is available on every task class and is meant for lightweight, automated
reporting (e.g. a daily Airflow report DAG).

It returns a dict:

| key | type | meaning |
| --- | --- | --- |
| `last_acquisition` | `pd.Timestamp` \| `None` | most recent `production_datetime` with at least one leadtime hit (`None` if no data) |
| `success_rate` | `float` \| `None` | `hit_leadtimes / total_leadtimes` at that date, in `[0, 1]` (`None` if no data) |
| `hit_leadtimes` | `int` | leadtimes with data at that date |
| `total_leadtimes` | `int` | leadtimes indexed for that date |

`refresh` (default `False`):
- `False` — reports from the current in-memory index; call after a `retrieve()`/`update()` in the same process.
- `True` — first rebuilds the index from **stored + local** files (`cloud=False`), which is fast and **network-free** (it relies on the `completeness.csv` sidecars). Use this for a standalone report.

### Docker CLI
`main.py` prints the returned dict as `Result: {...}`.
```bash
docker-compose run --rm tethys-tasks ICON_CH2_EPS_TOT_PREC acquisition_status --class_kwargs "{\"date_from\": \"'2026-04-07 12:00:00'\"}" --fun_kwargs "{\"refresh\": true}"
```

### Outside docker
```bash
python main.py ERA5_ZAMBEZI_T2M acquisition_status --class_kwargs "{\"date_from\": \"'2025-10-01'\"}" --fun_kwargs "{\"refresh\": true}"
```

### In Python (e.g. an Airflow report DAG)
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
```