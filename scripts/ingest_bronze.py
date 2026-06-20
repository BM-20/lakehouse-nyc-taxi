"""Bronze layer: append one month of raw NYC Yellow Taxi data to Iceberg.

Incremental and idempotent. Each run appends a single month as a new Iceberg
snapshot (so time-travel history is real), downloading the source file from the
public NYC TLC host if it isn't already in data/raw/, and skipping any month
that has already been loaded.

Usage:
    python scripts/ingest_bronze.py --month 2024-01
"""
from __future__ import annotations

import argparse
from datetime import datetime

import pyarrow as pa
import pyarrow.parquet as pq

from lakehouse_common import (
    ensure_local_parquet,
    get_catalog,
    source_file,
    source_file_loaded,
)

TABLE_NAME = "bronze.yellow_taxi_raw"
TABLE_LOCATION = "gs://lakehouse-nyc-taxi/iceberg/bronze/yellow_taxi_raw"


def run(month: str) -> None:
    """Append one month (e.g. '2024-01') of raw trip data to the bronze table."""
    catalog = get_catalog()
    file = source_file(month)

    # Create bronze namespace if it doesn't exist
    if ("bronze",) not in catalog.list_namespaces():
        catalog.create_namespace("bronze")

    # Idempotency: skip months already present so Airflow retries / re-runs are
    # safe no-ops.
    table = catalog.load_table(TABLE_NAME) if catalog.table_exists(TABLE_NAME) else None
    if table is not None and source_file_loaded(table, file):
        print(f"⏭️  {file} already loaded into {TABLE_NAME} — skipping.")
        return

    # Read source parquet (downloading from NYC TLC if not present locally)
    path = ensure_local_parquet(month)
    print(f"📂 Reading {path.name} ...")
    df = pq.read_table(path)

    # Add bronze layer metadata columns so every row can be traced back to its
    # origin file and ingestion time.
    print("➕ Adding metadata columns...")
    df = df.append_column(
        "_source_file",
        pa.array([file] * len(df), type=pa.string()),
    )
    df = df.append_column(
        "_ingested_at",
        pa.array([datetime.now()] * len(df), type=pa.timestamp("us")),
    )

    # Create the table on first run (schema inferred); otherwise append a new
    # snapshot to the existing table.
    if table is None:
        print(f"📝 Creating table {TABLE_NAME}...")
        table = catalog.create_table(
            TABLE_NAME,
            schema=df.schema,
            location=TABLE_LOCATION,
        )

    print(f"⬆️  Appending {len(df):,} rows to GCS...")
    table.append(df)

    # Reload to report the up-to-date snapshot count.
    table = catalog.load_table(TABLE_NAME)
    print(f"""
✅ Bronze layer updated!
   Table:     {TABLE_NAME}
   Month:     {month}
   Rows added: {len(df):,}
   Snapshots:  {len(table.metadata.snapshots)}
   Location:   {TABLE_LOCATION}
""")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Append a month of raw NYC taxi data to the bronze Iceberg table."
    )
    parser.add_argument("--month", required=True, help="Month to load, e.g. 2024-01")
    run(parser.parse_args().month)


if __name__ == "__main__":
    main()
