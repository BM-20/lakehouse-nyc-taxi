# Demonstrates Apache Iceberg's time travel feature.

from pyiceberg.catalog.sql import SqlCatalog

# Connect to the same Iceberg catalog used in the ingestion scripts
catalog = SqlCatalog(
    "nyc_taxi_catalog",
    **{
        "uri": "sqlite:///catalog/iceberg_catalog.db",
        "warehouse": "gs://lakehouse-nyc-taxi/iceberg",
    }
)

# Load the bronze table
table = catalog.load_table("bronze.yellow_taxi_raw")

# Each one represents a point in time the table existed
print("📜 Snapshot history for bronze.yellow_taxi_raw:\n")
for snapshot in table.metadata.snapshots:
    print(f"   Snapshot ID: {snapshot.snapshot_id}")
    print(f"   Timestamp:   {snapshot.timestamp_ms}")
    print(f"   Operation:   {snapshot.summary.operation if snapshot.summary else 'N/A'}")
    print()

# Query the table using its current snapshot
current_snapshot_id = table.metadata.current_snapshot_id
print(f"🔍 Querying current snapshot ({current_snapshot_id})...")
current_df = table.scan(snapshot_id=current_snapshot_id).to_arrow()
print(f"   Row count at current snapshot: {len(current_df):,}\n")

# If more than one snapshot exists, demonstrate querying an earlier one
if len(table.metadata.snapshots) > 1:
    earliest_snapshot_id = table.metadata.snapshots[0].snapshot_id
    print(f"🕐 Querying earliest snapshot ({earliest_snapshot_id})...")
    earliest_df = table.scan(snapshot_id=earliest_snapshot_id).to_arrow()
    print(f"   Row count at earliest snapshot: {len(earliest_df):,}\n")
else:
    print("ℹ️  Only one snapshot exists — rerun ingest_bronze.py to create a second snapshot and see time travel in action.")

print("✅ Time travel demo complete!")