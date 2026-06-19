#Connects to the same Iceberg catalog used in bronze and silver layers
from pyiceberg.catalog.sql import SqlCatalog

# PyArrow is used to read the silver table and compute aggregations
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

# Create gold namespace if it already doesnt exist
if ("gold",) not in catalog.list_namespaces():
    catalog.create_namespace("gold")

# Read silver table
print("📂 Reading silver layer...")
silver_table = catalog.load_table("silver.yellow_taxi_cleaned")
df = silver_table.scan().to_arrow()
print(f"   Rows loaded: {len(df):,}")

print("🔨 Building trips_by_hour...")

hours = df["pickup_hour"]
unique_hours = sorted(set(hours.to_pylist()))

# For each hour (0-23), calculate total trips, average fare and average distance
trips_by_hour = pa.table({
    "pickup_hour": pa.array(unique_hours, type=pa.int64()),
    "total_trips": pa.array([
        pc.sum(pc.equal(hours, h).cast(pa.int64())).as_py()
        for h in unique_hours
    ], type=pa.int64()),
    "avg_fare": pa.array([
        round(pc.mean(df["fare_amount"].filter(pc.equal(hours, h))).as_py(), 2)
        for h in unique_hours
    ], type=pa.float64()),
    "avg_trip_distance": pa.array([
        round(pc.mean(df["trip_distance"].filter(pc.equal(hours, h))).as_py(), 2)
        for h in unique_hours
    ], type=pa.float64()),
    "_created_at": pa.array([datetime.now()] * len(unique_hours), type=pa.timestamp('us'))
})


print("🔨 Building trips_by_payment...")

payment_types = df["payment_type"]
unique_payments = sorted(set(payment_types.to_pylist()))

trips_by_payment = pa.table({
    "payment_type": pa.array(unique_payments, type=pa.string()),
    "total_trips": pa.array([
        pc.sum(pc.equal(payment_types, p).cast(pa.int64())).as_py()
        for p in unique_payments
    ], type=pa.int64()),
    "total_revenue": pa.array([
        round(pc.sum(df["total_amount"].filter(pc.equal(payment_types, p))).as_py(), 2)
        for p in unique_payments
    ], type=pa.float64()),
    "avg_tip": pa.array([
        round(pc.mean(df["tip_amount"].filter(pc.equal(payment_types, p))).as_py(), 2)
        for p in unique_payments
    ], type=pa.float64()),
    "_created_at": pa.array([datetime.now()] * len(unique_payments), type=pa.timestamp('us'))
})

print("🔨 Building vendor_summary...")

vendors = df["vendor_name"]
unique_vendors = sorted(set(vendors.to_pylist()))

vendor_summary = pa.table({
    "vendor_name": pa.array(unique_vendors, type=pa.string()),
    "total_trips": pa.array([
        pc.sum(pc.equal(vendors, v).cast(pa.int64())).as_py()
        for v in unique_vendors
    ], type=pa.int64()),
    "avg_fare": pa.array([
        round(pc.mean(df["fare_amount"].filter(pc.equal(vendors, v))).as_py(), 2)
        for v in unique_vendors
    ], type=pa.float64()),
    "avg_distance": pa.array([
        round(pc.mean(df["trip_distance"].filter(pc.equal(vendors, v))).as_py(), 2)
        for v in unique_vendors
    ], type=pa.float64()),
    "avg_duration_minutes": pa.array([
        round(pc.mean(df["trip_duration_minutes"].filter(pc.equal(vendors, v))).as_py(), 2)
        for v in unique_vendors
    ], type=pa.float64()),
    "_created_at": pa.array([datetime.now()] * len(unique_vendors), type=pa.timestamp('us'))
})

print("🔨 Building daily_summary...")

# Extract date from pickup datetime
dates = pc.cast(pc.floor_temporal(df["pickup_datetime"], unit="day"), pa.date32())
unique_dates = sorted(set(dates.to_pylist()))

daily_summary = pa.table({
    "pickup_date": pa.array(unique_dates, type=pa.date32()),
    "total_trips": pa.array([
        pc.sum(pc.equal(dates, d).cast(pa.int64())).as_py()
        for d in unique_dates
    ], type=pa.int64()),
    "total_revenue": pa.array([
        round(pc.sum(df["total_amount"].filter(pc.equal(dates, d))).as_py(), 2)
        for d in unique_dates
    ], type=pa.float64()),
    "avg_fare": pa.array([
        round(pc.mean(df["fare_amount"].filter(pc.equal(dates, d))).as_py(), 2)
        for d in unique_dates
    ], type=pa.float64()),
    "avg_trip_distance": pa.array([
        round(pc.mean(df["trip_distance"].filter(pc.equal(dates, d))).as_py(), 2)
        for d in unique_dates
    ], type=pa.float64()),
    "_created_at": pa.array([datetime.now()] * len(unique_dates), type=pa.timestamp('us'))
})

# Map each gold table name to its corresponding PyArrow table built above

gold_tables = {
    "gold.trips_by_hour": trips_by_hour,
    "gold.trips_by_payment": trips_by_payment,
    "gold.vendor_summary": vendor_summary,
    "gold.daily_summary": daily_summary,
}

for table_name, data in gold_tables.items():
    if catalog.table_exists(table_name):
        print(f"🔄 {table_name} exists — overwriting...")
        catalog.drop_table(table_name)

    folder = table_name.replace(".", "/")
    table = catalog.create_table(
        table_name,
        schema=data.schema,
        location=f"gs://lakehouse-nyc-taxi/iceberg/{folder}"
    )
    table.append(data)
    print(f"✅ {table_name} written — {len(data):,} rows")

print(f"""
✅ Gold layer complete!
   Tables written:
   - gold.trips_by_hour      ({len(trips_by_hour)} rows)
   - gold.trips_by_payment   ({len(trips_by_payment)} rows)
   - gold.vendor_summary     ({len(vendor_summary)} rows)
   - gold.daily_summary      ({len(daily_summary)} rows)
   Location: gs://lakehouse-nyc-taxi/iceberg/gold/
""")