"""Gold layer: rebuild the business aggregation tables from the silver layer.

The gold tables are global aggregates (by hour, payment type, vendor, and date),
so they must reflect *all* silver data, not just the latest month. Each run
recomputes them from the full silver table and overwrites the gold tables in
place (one new snapshot each), which is both correct and idempotent.

Aggregation is done with DuckDB ``GROUP BY`` over the silver data rather than
per-group Python loops, so a full recompute stays fast and light as silver grows
month over month.

Usage:
    python scripts/ingest_gold.py
"""
from __future__ import annotations

import argparse
from datetime import datetime

import duckdb
import pyarrow as pa

from lakehouse_common import get_catalog

SILVER_TABLE = "silver.yellow_taxi_cleaned"

# Each gold table: the aggregation SQL (over a DuckDB view named `silver`) and
# its GCS location.
GOLD_SPECS = {
    "gold.trips_by_hour": {
        "location": "gs://lakehouse-nyc-taxi/iceberg/gold/trips_by_hour",
        "cols": ("pickup_hour", "fare_amount", "trip_distance"),
        "sql": """
            SELECT
                pickup_hour,
                COUNT(*)                      AS total_trips,
                ROUND(AVG(fare_amount), 2)    AS avg_fare,
                ROUND(AVG(trip_distance), 2)  AS avg_trip_distance
            FROM silver
            GROUP BY pickup_hour
            ORDER BY pickup_hour
        """,
    },
    "gold.trips_by_payment": {
        "location": "gs://lakehouse-nyc-taxi/iceberg/gold/trips_by_payment",
        "cols": ("payment_type", "total_amount", "tip_amount"),
        "sql": """
            SELECT
                payment_type,
                COUNT(*)                       AS total_trips,
                ROUND(SUM(total_amount), 2)    AS total_revenue,
                ROUND(AVG(tip_amount), 2)      AS avg_tip
            FROM silver
            GROUP BY payment_type
            ORDER BY payment_type
        """,
    },
    "gold.vendor_summary": {
        "location": "gs://lakehouse-nyc-taxi/iceberg/gold/vendor_summary",
        "cols": ("vendor_name", "fare_amount", "trip_distance", "trip_duration_minutes"),
        "sql": """
            SELECT
                vendor_name,
                COUNT(*)                              AS total_trips,
                ROUND(AVG(fare_amount), 2)            AS avg_fare,
                ROUND(AVG(trip_distance), 2)          AS avg_distance,
                ROUND(AVG(trip_duration_minutes), 2)  AS avg_duration_minutes
            FROM silver
            GROUP BY vendor_name
            ORDER BY vendor_name
        """,
    },
    "gold.daily_summary": {
        "location": "gs://lakehouse-nyc-taxi/iceberg/gold/daily_summary",
        "cols": ("pickup_datetime", "total_amount", "fare_amount", "trip_distance"),
        "sql": """
            SELECT
                CAST(pickup_datetime AS DATE)  AS pickup_date,
                COUNT(*)                       AS total_trips,
                ROUND(SUM(total_amount), 2)    AS total_revenue,
                ROUND(AVG(fare_amount), 2)     AS avg_fare,
                ROUND(AVG(trip_distance), 2)   AS avg_trip_distance
            FROM silver
            GROUP BY pickup_date
            ORDER BY pickup_date
        """,
    },
}


def _with_created_at(table: pa.Table) -> pa.Table:
    """Append a `_created_at` audit column to an aggregation result."""
    return table.append_column(
        "_created_at",
        pa.array([datetime.now()] * len(table), type=pa.timestamp("us")),
    )


def run(month: str | None = None) -> None:
    """Recompute every gold table from the full silver layer. `month` is ignored
    (gold is a global recompute) but accepted so the step has a uniform signature."""
    catalog = get_catalog()
    silver_tbl = catalog.load_table(SILVER_TABLE)

    # Each gold table is recomputed from *all* of silver, which grows month over
    # month. Rather than materialising the whole table (which OOMs once it spans
    # many months), run each aggregation as a streaming GROUP BY directly over a
    # fresh Arrow batch reader, projected to just the columns it needs. DuckDB
    # then only ever holds the small group state, never the millions of rows.
    con = duckdb.connect()
    for table_name, spec in GOLD_SPECS.items():
        print(f"🔨 Building {table_name} (streaming silver)...")
        reader = silver_tbl.scan(selected_fields=spec["cols"]).to_arrow_batch_reader()
        con.register("silver", reader)
        data = _with_created_at(con.execute(spec["sql"]).to_arrow_table())
        con.unregister("silver")

        if catalog.table_exists(table_name):
            # Replace all rows in a single new snapshot (idempotent refresh).
            catalog.load_table(table_name).overwrite(data)
        else:
            table = catalog.create_table(
                table_name,
                schema=data.schema,
                location=spec["location"],
            )
            table.append(data)
        print(f"✅ {table_name} written — {len(data):,} rows")
    con.close()

    print("""
✅ Gold layer complete!
   Tables refreshed: gold.trips_by_hour, gold.trips_by_payment,
                     gold.vendor_summary, gold.daily_summary
   Location: gs://lakehouse-nyc-taxi/iceberg/gold/
""")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Rebuild the gold aggregation tables from the silver Iceberg table."
    )
    # Accepted for a uniform CLI across the three layers; gold ignores it.
    parser.add_argument("--month", required=False, help="Ignored — gold is a global recompute.")
    run(parser.parse_args().month)


if __name__ == "__main__":
    main()
