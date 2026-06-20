"""Shared configuration and helpers for the NYC Taxi lakehouse pipeline.

Centralises the Iceberg catalog connection (previously copy-pasted across the
bronze, silver and gold ingestion scripts), resolves project paths so the
scripts work no matter which directory they are launched from (important once
Airflow runs them), and handles fetching the monthly NYC TLC source files.
"""
from __future__ import annotations

import shutil
import urllib.request
from pathlib import Path

from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.expressions import EqualTo

# Resolve the project root from this file's location (scripts/ -> project root)
# so relative paths (catalog DB, data/raw) are stable regardless of the CWD the
# scripts are launched from.
PROJECT_ROOT = Path(__file__).resolve().parents[1]

# Local landing zone for the raw monthly parquet files.
RAW_DIR = PROJECT_ROOT / "data" / "raw"

# Public NYC TLC trip-data host. Files are named yellow_tripdata_YYYY-MM.parquet.
NYC_TLC_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

# Iceberg catalog connection details. The SQLite catalog path is made absolute
# so the same value works from the CLI and from inside the Airflow container.
CATALOG_NAME = "nyc_taxi_catalog"
WAREHOUSE = "gs://lakehouse-nyc-taxi/iceberg"
CATALOG_URI = f"sqlite:///{PROJECT_ROOT / 'catalog' / 'iceberg_catalog.db'}"


def get_catalog() -> SqlCatalog:
    """Connect to the shared Iceberg catalog used by every layer."""
    return SqlCatalog(
        CATALOG_NAME,
        **{
            "uri": CATALOG_URI,
            "warehouse": WAREHOUSE,
        },
    )


def source_file(month: str) -> str:
    """Source filename for a month: '2024-01' -> 'yellow_tripdata_2024-01.parquet'."""
    return f"yellow_tripdata_{month}.parquet"


def local_path(month: str) -> Path:
    """Local path to a month's raw parquet file under data/raw/."""
    return RAW_DIR / source_file(month)


def ensure_local_parquet(month: str) -> Path:
    """Return the local path to a month's parquet, downloading it if absent.

    Files are pulled from the public NYC TLC host (~50 MB/month). The download
    streams to a temporary ``.part`` file and is renamed into place only once
    complete, so an interrupted run never leaves a half-written file that looks
    valid on the next attempt.
    """
    path = local_path(month)
    if path.exists():
        print(f"📂 Using cached source file: {path.name}")
        return path

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    url = f"{NYC_TLC_BASE_URL}/{source_file(month)}"
    print(f"⬇️  Downloading {url} ...")
    tmp = path.with_name(path.name + ".part")
    request = urllib.request.Request(url, headers={"User-Agent": "lakehouse-nyc-taxi/1.0"})
    with urllib.request.urlopen(request) as resp, open(tmp, "wb") as out:
        shutil.copyfileobj(resp, out)
    tmp.replace(path)
    size_mb = path.stat().st_size / 1_000_000
    print(f"✅ Downloaded {path.name} ({size_mb:,.1f} MB)")
    return path


def source_file_loaded(table, file: str) -> bool:
    """True if rows tagged with this ``_source_file`` are already in the table.

    Used to make the bronze and silver steps idempotent, so Airflow retries and
    re-runs of an already-processed month are safe no-ops.
    """
    scan = table.scan(
        row_filter=EqualTo("_source_file", file),
        selected_fields=("_source_file",),
        limit=1,
    )
    return scan.to_arrow().num_rows > 0
