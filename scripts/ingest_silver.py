"""Silver layer: clean and enrich one month of bronze data into Iceberg.

Incremental — reads only the new month's rows from the bronze table (not the
whole table) using an Iceberg row filter, cleans and enriches them, and appends
the result to the silver table. Idempotent per month.

Usage:
    python scripts/ingest_silver.py --month 2024-01
"""
from __future__ import annotations

import argparse
from datetime import datetime

import pyarrow as pa
import pyarrow.compute as pc
from pyiceberg.expressions import EqualTo

from lakehouse_common import get_catalog, source_file, source_file_loaded

BRONZE_TABLE = "bronze.yellow_taxi_raw"
SILVER_TABLE = "silver.yellow_taxi_cleaned"
SILVER_LOCATION = "gs://lakehouse-nyc-taxi/iceberg/silver/yellow_taxi_cleaned"


def run(month: str) -> None:
    """Transform one month (e.g. '2024-01') of bronze data into the silver table."""
    catalog = get_catalog()
    file = source_file(month)

    # Create silver namespace if it doesn't exist
    if ("silver",) not in catalog.list_namespaces():
        catalog.create_namespace("silver")

    # Idempotency: skip months already transformed into silver.
    silver_exists = catalog.table_exists(SILVER_TABLE)
    if silver_exists and source_file_loaded(catalog.load_table(SILVER_TABLE), file):
        print(f"⏭️  {file} already present in {SILVER_TABLE} — skipping.")
        return

    # Read ONLY this month's rows from bronze (incremental read).
    print(f"📂 Reading {file} from bronze...")
    bronze_table = catalog.load_table(BRONZE_TABLE)
    df = bronze_table.scan(row_filter=EqualTo("_source_file", file)).to_arrow()
    print(f"   Rows loaded: {len(df):,}")
    if len(df) == 0:
        print(f"⚠️  No bronze rows for {file}. Run ingest_bronze.py --month {month} first.")
        return

    print("🧹 Cleaning data...")

    # Filter out bad trips by building a boolean mask
    mask = pc.and_(
        pc.and_(
            pc.and_(
                pc.and_(
                    pc.greater(df["fare_amount"], 0),
                    pc.greater(df["trip_distance"], 0)
                ),
                pc.greater(df["passenger_count"], 0)
            ),
            pc.less_equal(df["trip_distance"], 100)
        ),
        pc.less_equal(df["fare_amount"], 500)
    )
    df = df.filter(mask)
    print(f"   Rows after cleaning: {len(df):,}")

    print("🔄 Adding calculated columns...")

    # Calculate trip duration in minutes from pickup and dropoff timestamps
    pickup = df["tpep_pickup_datetime"].cast(pa.int64())
    dropoff = df["tpep_dropoff_datetime"].cast(pa.int64())
    duration_us = pc.subtract(dropoff, pickup)
    duration_minutes = pc.round(
        pc.divide(duration_us.cast(pa.float64()), 60_000_000), 2
    )

    # Pickup hour and day of week
    pickup_ts = df["tpep_pickup_datetime"]
    pickup_hour = pc.hour(pickup_ts)
    pickup_day_of_week = pc.day_of_week(pickup_ts)

    # Decode payment type from its raw integer code to a human readable label
    payment_map = {1: "Credit Card", 2: "Cash", 3: "No Charge", 4: "Dispute", 5: "Unknown", 6: "Voided"}
    payment_labels = pa.array(
        [payment_map.get(x.as_py(), "Unknown") for x in df["payment_type"]],
        type=pa.string()
    )

    # Decode vendor ID from its raw integer code to the vendor's actual company name
    vendor_map = {1: "Creative Mobile Technologies", 2: "VeriFone"}
    vendor_labels = pa.array(
        [vendor_map.get(x.as_py(), "Unknown") for x in df["VendorID"]],
        type=pa.string()
    )

    print("🔨 Building silver table...")

    # Assemble the cleaned and enriched silver table
    silver_df = pa.table({
        # Trip identifiers
        "vendor_name":             vendor_labels,
        "pickup_datetime":         df["tpep_pickup_datetime"],
        "dropoff_datetime":        df["tpep_dropoff_datetime"],
        "pickup_hour":             pickup_hour,
        "pickup_day_of_week":      pickup_day_of_week,

        # Trip details
        "passenger_count":         df["passenger_count"],
        "trip_distance":           df["trip_distance"],
        "trip_duration_minutes":   duration_minutes,
        "pickup_location_id":      df["PULocationID"],
        "dropoff_location_id":     df["DOLocationID"],
        "ratecode_id":             df["RatecodeID"],
        "store_and_fwd_flag":      df["store_and_fwd_flag"],

        # Fares
        "fare_amount":             df["fare_amount"],
        "extra":                   df["extra"],
        "mta_tax":                 df["mta_tax"],
        "tip_amount":              df["tip_amount"],
        "tolls_amount":            df["tolls_amount"],
        "improvement_surcharge":   df["improvement_surcharge"],
        "congestion_surcharge":    df["congestion_surcharge"],
        "airport_fee":             df["Airport_fee"],
        "total_amount":            df["total_amount"],

        # Payment
        "payment_type":            payment_labels,

        # Metadata
        "_source_file":            df["_source_file"],
        "_ingested_at":            df["_ingested_at"],
        "_transformed_at":         pa.array(
                                       [datetime.now()] * len(df),
                                       type=pa.timestamp('us')
                                   ),
    })

    # Create the silver table on first run (schema inferred); otherwise append.
    if not silver_exists:
        print(f"📝 Creating table {SILVER_TABLE}...")
        table = catalog.create_table(
            SILVER_TABLE,
            schema=silver_df.schema,
            location=SILVER_LOCATION,
        )
    else:
        table = catalog.load_table(SILVER_TABLE)

    print(f"⬆️  Appending {len(silver_df):,} rows to GCS...")
    table.append(silver_df)

    print(f"""
✅ Silver layer updated!
   Table:     {SILVER_TABLE}
   Month:     {month}
   Rows in:   {len(df):,}
   Rows out:  {len(silver_df):,}
   Location:  {SILVER_LOCATION}
""")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Clean and enrich a month of bronze data into the silver Iceberg table."
    )
    parser.add_argument("--month", required=True, help="Month to transform, e.g. 2024-01")
    run(parser.parse_args().month)


if __name__ == "__main__":
    main()
