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
```

### Run a specific class function (built image)
Uses the code baked into the image.
```bash
docker-compose run --rm tethys-tasks C3S_ECMWF_TPRATE_IBERIA retrieve_store_and_upload --class_kwargs "{\"date_from\": \"'2025-12-01'\", \"download_from_source\": \"True\"}" 
--fun_kwargs "{}"

docker-compose run --rm tethys-tasks ERA5_TP_CAUCASUS retrieve_store_upload_and_cleanup --class_kwargs "{\"date_from\": \"'2025-05-01'\", \"download_from_source=True\": \"True\"}"

docker-compose run --rm tethys-tasks ERA5_ZAMBEZI_TP store --class_kwargs "{\"date_from\": \"'2025-05-01'\", \"download_from_source=False\": \"False\"}" 


docker-compose run --rm tethys-tasks ALARO40L_T2M retrieve_and_upload --class_kwargs "{\"date_from\": \"'2025-05-01'\", \"download_from_source=True\": \"True\"}"
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