A data lakehouse built on Apache Iceberg, processing real NYC taxi trip data through bronze, silver and gold layers, with dbt and Looker Studio on top.

## Project Overview

This project ingests real NYC Yellow Taxi trip data — January–October 2024, ~33.8 million trips (~29.3M after cleaning) loaded month by month — and processes it through a medallion architecture using Apache Iceberg on Google Cloud Storage. Raw data lands in a bronze layer, gets cleaned and enriched in silver, then aggregated into business ready gold tables. BigQuery reads the gold layer as native Iceberg external tables, and dbt builds a staging and marts layer on top with tests and full lineage documentation. A Looker Studio dashboard visualises trip demand, revenue, and vendor activity.

Loading is **incremental and idempotent** — the pipeline works a month at a time, appending each new month as a fresh Iceberg snapshot and safely skipping months already loaded — and the whole flow (bronze → silver → gold → dbt) is **orchestrated by Apache Airflow** on a monthly schedule running in Docker.

### Stack

| Component | Tool | Notes |
|---|---|---|
| Source data | NYC TLC Yellow Taxi Trips | Monthly parquet files, auto-downloaded (~33.8M trips across Jan–Oct 2024) |
| Table format | Apache Iceberg | ACID transactions, schema enforcement, snapshot history |
| Processing | Python (PyArrow, PyIceberg, DuckDB) | Incremental, month-parameterized bronze/silver/gold scripts |
| Storage | Google Cloud Storage | Iceberg tables stored as managed GCS objects |
| Orchestration | Apache Airflow (Docker) | Monthly DAG driving bronze → silver → gold → dbt; each run = one month |
| Analytics | BigQuery (Iceberg external tables) | Queries Iceberg metadata directly, no data duplication |
| Transformations | dbt (BigQuery adapter) | Staging and mart models with tests and lineage docs |
| Visualisation | Looker Studio | Trip demand, revenue and vendor activity |

## Project Implementation

Each script takes a `--month` (e.g. `2024-01`) and is **incremental and idempotent**: it processes only that month and is a safe no-op if the month is already loaded, so Airflow retries and re-runs never duplicate data.

### Bronze Layer
The month's source Parquet is read (auto-downloaded from the public NYC TLC host into `data/raw/` if missing) and **appended** to the Iceberg table exactly as received — each month becomes a new snapshot rather than overwriting the table. Two metadata columns are added (`_source_file`, `_ingested_at`) so every row can be traced back to its origin and ingestion time, and `_source_file` is what makes the load idempotent.

### Silver Layer
Only the new month's rows are read from bronze (an Iceberg row-filter scan on `_source_file`, not a full-table read), then cleaned and standardised before being **appended** to the silver table. Invalid trips are filtered out (zero or negative fares, zero distance, zero passengers, distance and fare outliers, and trips whose pickup timestamp falls outside the file's own month — a handful of source rows carry corrupt meter clocks). Calculated columns are added (`trip_duration_minutes`, `pickup_hour`, `pickup_day_of_week`), and `payment_type` and `VendorID` are decoded from raw integer codes into readable labels.

### Gold Layer
Because the aggregates are global, the gold tables are recomputed from the **full** silver layer and **overwritten** in place on each run — correct by construction and idempotent, while still recording a new Iceberg snapshot. Each aggregate runs as a **streaming** DuckDB `GROUP BY` over the silver layer (projected to just the columns it needs), so memory stays bounded even as the data grows to tens of millions of rows. Four business ready aggregation tables are built:
- `trips_by_hour` — demand and average fare by hour of day
- `trips_by_payment` — revenue and tipping behaviour by payment method
- `vendor_summary` — trip volume and fare comparison between vendors
- `daily_summary` — daily trip volume and revenue trends

### Transformations
A dbt project sits on top of the four gold tables, exposed to BigQuery as Iceberg external tables. Staging models clean and filter the data further, and mart models (`peak_hours`, `revenue_by_payment`, `vendor_comparison`, `weekly_trends`) add business logic such as time period categorisation and day over day deltas.


## Prerequisites

- Python 3.9+
- A [GCP account](https://cloud.google.com) with billing enabled (free tier sufficient), a GCS bucket for the warehouse, and gcloud Application Default Credentials: `gcloud auth application-default login`
- [Docker](https://docs.docker.com/get-docker/) + Docker Compose (only for the Airflow path; the image build needs ~3–5 GiB of free disk)
- Python packages (the [NYC TLC trip data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) is auto-downloaded per month, so no manual download is needed):

```bash
pip install -r requirements.txt
```


## Execution

### Run manually, one month at a time

```bash
# Bronze: append a month (downloads it if not already in data/raw/)
python scripts/ingest_bronze.py --month 2024-01

# Silver: clean & enrich just that month's new rows
python scripts/ingest_silver.py --month 2024-01

# Gold: recompute the aggregates from the full silver layer
python scripts/ingest_gold.py

# Load the next month — re-running an already-loaded month is a safe no-op
python scripts/ingest_bronze.py --month 2024-02
python scripts/ingest_silver.py --month 2024-02
python scripts/ingest_gold.py

# dbt transformations on top of the gold layer
cd dbt_nyc_taxi
dbt run
dbt test
dbt docs generate
dbt docs serve
```

### Run with Airflow (Docker)

The Airflow stack runs the same scripts on a monthly schedule — each DAG run loads
one month (derived from its logical date), so backfilling the DAG loads the months
incrementally, one snapshot at a time.

```bash
cp .env.example .env          # then set AIRFLOW_UID (see the file)
docker compose build
docker compose up -d
# open http://localhost:8080  (login: airflow / airflow), then unpause `nyc_taxi_lakehouse`
```

GCS and BigQuery auth reuse your local gcloud ADC and `~/.dbt/profiles.yml`, mounted
read-only into the containers. The dbt tasks expect the BigQuery Iceberg external tables
over the gold layer to already exist; set `RUN_DBT=false` in `.env` to skip them and run
only bronze → silver → gold.

> **Heads up:** loading a new month appends real data to your GCS warehouse and grows the
> bronze/silver tables (changing the row counts cited above). `catchup=True` means unpausing
> the DAG will try to backfill every month from Jan 2024 to now — pause it once you've loaded
> the months you want. NYC TLC only publishes past months, so runs for not-yet-released months
> will fail on the download.

## Project Structure

```
lakehouse-project/
├── data/
│   └── raw/
│       └── yellow_tripdata_2024-01.parquet   # + any months you load (auto-downloaded)
├── scripts/
│   ├── lakehouse_common.py   # shared catalog/config + NYC TLC download + idempotency helper
│   ├── ingest_bronze.py      # --month: append a month (idempotent)
│   ├── ingest_silver.py      # --month: transform that month's new rows
│   ├── ingest_gold.py        # recompute aggregates from full silver (DuckDB)
│   └── iceberg_time_travel.py
├── airflow/
│   ├── Dockerfile            # Airflow image + project deps
│   └── dags/
│       └── nyc_taxi_lakehouse_dag.py   # monthly bronze→silver→gold→dbt DAG
├── docker-compose.yaml       # Airflow (LocalExecutor + Postgres)
├── requirements.txt
├── .env.example
├── catalog/
│   ├── iceberg_catalog.yaml 
│   └── iceberg_catalog.db  
├── dbt_nyc_taxi/
│   ├── dbt_project.yml
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_daily_summary.sql
│   │   │   ├── stg_trips_by_hour.sql
│   │   │   ├── stg_trips_by_payment.sql
│   │   │   ├── stg_vendor_summary.sql
│   │   │   ├── schema.yml
│   │   │   └── sources.yml
│   │   └── marts/
│   │       ├── peak_hours.sql
│   │       ├── revenue_by_payment.sql
│   │       ├── vendor_comparison.sql
│   │       ├── weekly_trends.sql
│   │       └── schema.yml
└── screenshots/
    ├── airflow_grid.png        
    ├── gcs_layers.png          
    ├── iceberg_time_travel.png 
    ├── bigquery_results.png    
    ├── dbt_lineage.png        
    └── looker_dashboard.png
```

## Evaluation

### Airflow Orchestration
![Airflow DAG runs](screenshots/airflow_grid.png)
*The `nyc_taxi_lakehouse` DAG in Airflow's grid view — one green run per month (Jan–Oct 2024), each chaining ingest_bronze → ingest_silver → ingest_gold → dbt_run → dbt_test. The monthly schedule with catchup is what drives the incremental load.*

### GCS Iceberg Layers
![GCS Layers](screenshots/gcs_layers.png)
*Bronze, silver and gold Iceberg tables stored in GCS, each with its own data/ and metadata/ folder confirming proper Iceberg table structure*

### BigQuery Results
![BigQuery Results](screenshots/bigquery_results.png)
*daily_summary gold table queried in BigQuery, recognised with the Lakehouse badge confirming native Iceberg support*

### dbt Lineage Graph
![dbt Lineage](screenshots/dbt_lineage.png)
*Full lineage across all four pipelines, gold source tables → staging → marts*

### Looker Studio Dashboard
![Looker Dashboard](screenshots/looker_dashboard.png)
*NYC Taxi Lakehouse dashboard showing taxi demand by hour, revenue share by payment method, trip volume by vendor, and daily trip trends across January–October 2024.*

## Future Work

- **Incremental loading** ✅ — done: bronze/silver/gold are month-parameterized, idempotent, and append new months as Iceberg snapshots
- **Airflow orchestration** ✅ — done: a monthly DAG (Docker) chains bronze → silver → gold → dbt, one month per run
- **Failure alerting** — wire the DAG up to email/Slack notifications on task failure
- **Schema evolution** — show Iceberg handling a new column added to a future data load without breaking existing queries
- **Snapshot expiry / compaction** — add maintenance to expire old snapshots and compact small files as months accumulate
- **True incremental gold** — merge per-key aggregates instead of recomputing from full silver, for when silver grows beyond a single-machine recompute

## Conclusion

This project demonstrates a governed data lakehouse built on Apache Iceberg, processing real NYC taxi trip data through a bronze, silver and gold medallion architecture. Unlike a simple file based pipeline, every layer is a proper Iceberg table with schema enforcement and snapshot history, queryable directly from BigQuery without data duplication.

The combination of layered data quality, dbt transformations with tests and documentation, and a clear separation between raw, cleaned and business ready data reflects how analytics engineering teams structure modern data platforms.