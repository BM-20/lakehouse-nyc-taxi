# Connects to the same Iceberg catalog used in the bronze layer
from pyiceberg.catalog.sql import SqlCatalog

# PyArrow is used to read the bronze table and perform vectorised
import pyarrow as pa
import pyarrow.compute as pc

from datetime import datetime

# Connect to catalog
catalog = SqlCatalog(
    "nyc_taxi_catalog",
    **{
        "uri": "sqlite:///catalog/iceberg_catalog.db",
        "warehouse": "gs://lakehouse-nyc-taxi/iceberg",
    }
)

# Create silver namespace if it doesn't exist
if ("silver",) not in catalog.list_namespaces():
    catalog.create_namespace("silver")

# Read the entire bronze table into memory as a PyArrow table
print("📂 Reading bronze layer...")
bronze_table = catalog.load_table("bronze.yellow_taxi_raw")
df = bronze_table.scan().to_arrow()
print(f"   Rows loaded: {len(df):,}")

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

# write to iceberg

table_name = "silver.yellow_taxi_cleaned"

if catalog.table_exists(table_name):
    print(f"🔄 Table {table_name} exists — overwriting...")
    catalog.drop_table(table_name)

print(f"📝 Creating table {table_name}...")
table = catalog.create_table(
    table_name,
    schema=silver_df.schema,
    location="gs://lakehouse-nyc-taxi/iceberg/silver/yellow_taxi_cleaned"
)

print(f"⬆️  Writing {len(silver_df):,} rows to GCS...")
table.append(silver_df)

print(f"""
✅ Silver layer complete!
   Table: {table_name}
   Rows in:  {len(df):,}
   Rows out: {len(silver_df):,}
   Columns added: trip_duration_minutes, pickup_hour, pickup_day_of_week,
                  vendor_name, payment_type (decoded)
   Columns renamed: Airport_fee → airport_fee
   Location: gs://lakehouse-nyc-taxi/iceberg/silver/
""")