# Tethys-tasks
Code to run tasks associated with Tethys

## Docker Usage
This project is designed to be run within a Docker container, typically orchestrated by an external Airflow instance.

### Build the image
```bash
docker build -t tethys-tasks:latest .
```

### Run a specific class function (built image)
Uses the code baked into the image.
```bash
docker-compose run --rm tethys-tasks ERA5_ZAMBEZI_TP retrieve_and_upload 
--class_args "[\"True\"]" --class_kwargs "{\"date_from\": \"'2025-05-01'\", \"download_from_source=False\": \"False\"}" 
--fun_kwargs "{}"


docker-compose run --rm tethys-tasks ERA5_ZAMBEZI_TP store --class_kwargs "{\"date_from\": \"'2025-05-01'\", \"download_from_source=False\": \"False\"}" 
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