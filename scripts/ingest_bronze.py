# Connects to the Iceberg catalog and manages table creation/writes
from pyiceberg.catalog.sql import SqlCatalog

# Reads the source Parquet file and builds the PyArrow table used for ingestion
import pyarrow.parquet as pq
import pyarrow as pa

# Used to timestamp when each row was ingested
from datetime import datetime

# Connect to the catalog
catalog = SqlCatalog(
    "nyc_taxi_catalog",
    **{
        "uri": "sqlite:///catalog/iceberg_catalog.db",
        "warehouse": "gs://lakehouse-nyc-taxi/iceberg",
    }
)

# Create bronze namespace if it doesn't exist
if ("bronze",) not in catalog.list_namespaces():
    catalog.create_namespace("bronze")

# Read source Parquet file into a PyArrow table
print("📂 Reading source Parquet file...")
df = pq.read_table("data/raw/yellow_tripdata_2024-01.parquet")

# Add bronze layer metadata columns
print("➕ Adding metadata columns...")
df = df.append_column(
    "_source_file",
    pa.array(["yellow_tripdata_2024-01.parquet"] * len(df), type=pa.string())
)
df = df.append_column(
    "_ingested_at",
    pa.array([datetime.now()] * len(df), type=pa.timestamp('us'))
)

# Create the bronze table or drop bronze table if it already exists
table_name = "bronze.yellow_taxi_raw"

if catalog.table_exists(table_name):
    print(f"🔄 Table {table_name} exists — overwriting...")
    catalog.drop_table(table_name)

#Create the Iceberg table, the schema is automatically inferred from the PyArrow table
print(f"📝 Creating table {table_name}...")
table = catalog.create_table(
    table_name,
    schema=df.schema,
    location=f"gs://lakehouse-nyc-taxi/iceberg/bronze/yellow_taxi_raw"
)
# if catalog.table_exists(table_name):
#     print(f"📸 Table {table_name} exists — appending to create new snapshot...")
#     table = catalog.load_table(table_name)
# else:
#     print(f"📝 Creating table {table_name}...")
#     table = catalog.create_table(
#         table_name,
#         schema=df.schema,
#         location=f"gs://lakehouse-nyc-taxi/iceberg/bronze/yellow_taxi_raw"
#     )
#This creates the data and metadata folders in GCS
print(f"⬆️  Writing {len(df):,} rows to GCS...")
table.append(df)

print(f"""
✅ Bronze layer complete!
   Table: {table_name}
   Rows: {len(df):,}
   Location: gs://lakehouse-nyc-taxi/iceberg/bronze/
   Metadata columns: _source_file, _ingested_at
""")