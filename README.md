# DWD Solar – Databricks Workflow (Download → Unzip → Medallion)

This workflow downloads hourly solar radiation data from the DWD Open Data portal,
unzips it into a Unity Catalog volume, and processes it through a Medallion pipeline
(Bronze → Silver → Gold) up to a comparison of global radiation and actual
photovoltaic power generation.

## Architecture

```
Task 1: download   fetch ZIPs from DWD (parallel, retry, size-skip)
Task 2: unzip      extract into the volume (Python zipfile, fails on corrupt ZIPs)
Task 3: bronze     raw data 1:1 as Delta + load metadata
Task 4: silver     cleaned & typed (trim, German number formats, timestamps)
Task 5: gold       equi-join radiation × actual PV generation at hourly granularity
```

Result table (Gold): `solar_pv_compare`

Visualization of results in 06_explore.ipynb

## Environment-Specific Parameters

All parameters are set via **job parameters**, which Databricks automatically passes
through to the `dbutils.widgets` of the notebook tasks. When running a notebook
manually (ad hoc), the widget defaults apply.

### Unity Catalog (data storage)

| Parameter      | Default | Meaning / how to adapt                                                               |
|----------------|---------|--------------------------------------------------------------------------------------|
| `catalog`      | `energy_weather` | UC catalog. Must exist in the workspace or be created (`CREATE_VOLUME=true` in the download task). |
| `schema`       | `default` | UC schema. Created by the download task if needed (`CREATE SCHEMA IF NOT EXISTS`). |
| `volume`       | `energy_weather` | UC volume. Created by the download task if needed (`CREATE VOLUME IF NOT EXISTS`). |
| `subfolder`    | `solar_zip/dwd_solar_hourly` | Subfolder in the volume for the downloaded ZIPs. No leading `/` (it is stripped anyway). |
| `dest_folder`  | `solar_unzipped` | Subfolder in the volume for the extracted DWD files (source for Bronze). |
| `realerz_path` | `Realisierte_Erzeugung_202501010000_202601010000_Stunde.csv` | Path of the actual-generation CSV **relative to the volume root**. Must be obtained separately and placed into the volume (e.g. via upload or a separate download). |

At runtime these resolve to the following paths (assembled identically in every task):

```
ZIP folder:      /Volumes/{catalog}/{schema}/{volume}/{subfolder}
Extracted files: /Volumes/{catalog}/{schema}/{volume}/{dest_folder}
Actual generation: /Volumes/{catalog}/{schema}/{volume}/{realerz_path}
```

### Workspace paths (job definition)

The job JSON assumes the following workspace structure. The notebook paths are
**absolute** and must be adapted if the notebooks live elsewhere:

```
/Workspace/dwd_solar/01_download.py
/Workspace/dwd_solar/02_unzip.py
/Workspace/dwd_solar/03_bronze_ingest.py
/Workspace/dwd_solar/04_silver_clean.py
/Workspace/dwd_solar/05_gold_join.py
```

Change these in the job JSON field `notebook_task.notebook_path`. The easiest way to
import the notebooks is via the workspace UI (**Create → Import**) or
`databricks workspace import`.

### Processing

| Parameter      | Default | Meaning                                             |
|----------------|---------|-----------------------------------------------------|
| `year`         | `2025`  | Year under review for the gold join.               |
| `target_table` | `solar_pv_compare` | Gold target table: `{catalog}.{schema}.{target_table}`. |
| `max_workers`  | `8`     | Parallel downloads in the `download` task.         |
| `create_volume`| `true`  | Create catalog/schema/volume if they do not exist. |
| `skip_existing`| `true`  | Skip existing ZIPs with matching size (idempotent re-runs). |

### Cluster & schedule (job JSON)

Also environment-specific – review before running `databricks jobs create`:

- `job_clusters[0].new_cluster.spark_version` – e.g. `15.4.x-scala2.12`; the
  available Spark versions differ per workspace. `_metadata.file_path`
  (bronze task) requires Databricks Runtime **13+**; on older versions remove
  the `_source_file` column.
- `node_type_id` – `i3.xlarge` here as an example; available node types differ
  per cloud (AWS/Azure/GCP).
- `autoscale` – currently 2–8 workers; 2–4 are enough for small test runs.
- `schedule.quartz_cron_expression` – currently daily at 04:00 (`0 0 4 * * ?`),
  `timezone_id: Europe/Berlin`. DWD typically updates the hourly values once
  per day; adjust or pause the cron accordingly.

### External source

- DWD Open Data: `https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/solar/`
  (`BASE_URL` in the download task). The URL is deliberately kept as a constant
  in the task; it does not vary per environment. The download task requires
  **internet access from the cluster** (or from serverless compute) – in
  workspaces with network restrictions (Private Link, egress firewall),
  `opendata.dwd.de` must be on the allowlist.

## Deployment

1. Import the notebooks into `/Workspace/dwd_solar/` (adjust paths if needed).
2. Upload the actual-generation CSV into the volume root (`realerz_path`).
3. Adapt the job JSON to your environment (workspace paths, Spark version, node type, schedule).
4. Create the job:

   ```bash
   databricks jobs create --json-file dwd_solar_job.json
   ```

   Authentication: `databricks auth login --host https://<workspace-url>`
   (OAuth) or `DATABRICKS_HOST`/`DATABRICKS_TOKEN` (PAT).

5. Test run: `databricks jobs run-now --job-id <id>` – or in the UI
   **Workflows → Run now**.

## Data models

| Layer  | Table(s) | Content |
|--------|----------|---------|
| Bronze | `solar_stunden_bronze`, `solar_stationen_bronze`, `realerzeugung_bronze` | Raw data as strings + `_source_file`, `_load_ts`; no business transformations |
| Silver | `solar_stunden_silver`, `solar_stationen_silver`, `realerzeugung_silver` | Typed (`double`, `timestamp`, `date`), trimmed IDs, prepared `stunde` column (`date_trunc('hour', ...)`) |
| Gold   | `solar_pv_compare` | Radiation (station period resolved, year filtered) equi-joined with actual PV generation on `stunde` |

Notes:
- CSV columns containing umlauts/special characters (e.g. `[MWh]`, mojibake from
  ISO-8859-1) must be sanitized before `saveAsTable` (spaces/brackets → `_`,
  `ä/ö/ü` → `ae/oe/ue`, collapse repeated underscores, number collisions).
- Bronze metadata (`_load_ts`, `_source_file`) is dropped before the gold join;
  any remaining column-name collisions between the join sides are automatically
  renamed with the suffix `_re` and logged by the task.
- All tasks are idempotent (`overwrite` writes, size skip, `extractall`) –
  re-runs are safe. A bug in Silver/Gold only requires re-running from the
  affected task; no repeated download/unzip is needed.
