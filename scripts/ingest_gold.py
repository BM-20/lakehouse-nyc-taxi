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

    # Load the full silver table and expose it to DuckDB as a view.
    print("📂 Reading silver layer...")
    silver = catalog.load_table(SILVER_TABLE).scan().to_arrow()
    print(f"   Rows loaded: {len(silver):,}")

    con = duckdb.connect()
    con.register("silver", silver)

    for table_name, spec in GOLD_SPECS.items():
        print(f"🔨 Building {table_name}...")
        data = _with_created_at(con.execute(spec["sql"]).to_arrow_table())

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
