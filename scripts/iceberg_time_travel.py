"""Demonstrates Apache Iceberg's time travel feature.

Walks the bronze table's snapshot history — one snapshot per monthly load — and
shows how the table grew over time. Row counts are read straight from each
snapshot's Iceberg metadata (no table scan), so this runs in about a second even
though the table holds tens of millions of rows.

Run locally (where your gcloud ADC is set up):
    python scripts/iceberg_time_travel.py
"""
from datetime import datetime, timezone

from lakehouse_common import get_catalog

catalog = get_catalog()
table = catalog.load_table("bronze.yellow_taxi_raw")

snapshots = table.metadata.snapshots
current_id = table.metadata.current_snapshot_id


def _summary_int(summary, key):
    """Read a numeric field from a snapshot summary, defaulting to 0."""
    return int(summary.additional_properties.get(key, 0)) if summary else 0


print(f"📜 Snapshot history for bronze.yellow_taxi_raw — {len(snapshots)} snapshots\n")
print(f"   {'#':>2}  {'timestamp (UTC)':<19}  {'operation':<9}  {'rows added':>12}  {'total rows':>13}")
print(f"   {'--':>2}  {'-' * 19}  {'-' * 9}  {'-' * 12}  {'-' * 13}")
for i, snap in enumerate(snapshots, start=1):
    ts = datetime.fromtimestamp(snap.timestamp_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    op = getattr(snap.summary.operation, "value", "?") if snap.summary else "?"
    added = _summary_int(snap.summary, "added-records")
    total = _summary_int(snap.summary, "total-records")
    marker = "  ← current" if snap.snapshot_id == current_id else ""
    print(f"   {i:>2}  {ts:<19}  {op:<9}  {added:>12,}  {total:>13,}{marker}")

print()

if len(snapshots) > 1:
    first, last = snapshots[0], snapshots[-1]
    first_total = _summary_int(first.summary, "total-records")
    last_total = _summary_int(last.summary, "total-records")
    print(f"📈 The table grew from {first_total:,} rows (first load) to "
          f"{last_total:,} rows across {len(snapshots)} snapshots.")

    # If the current pointer isn't the newest snapshot, a rollback happened —
    # Iceberg keeps the rolled-back snapshot in history, which is time travel.
    if current_id != last.snapshot_id:
        current_total = _summary_int(table.metadata.current_snapshot().summary, "total-records")
        print(f"↩️  Current pointer is rolled back to {current_total:,} rows "
              f"(snapshot {current_id}); later snapshots remain queryable.")

    print(f"\n🕐 Query the table as of any past snapshot, e.g. its first load:")
    print(f"   table.scan(snapshot_id={first.snapshot_id}).to_arrow()")
else:
    print("ℹ️  Only one snapshot exists — load another month to see time travel in action.")

print("\n✅ Time travel demo complete!")
