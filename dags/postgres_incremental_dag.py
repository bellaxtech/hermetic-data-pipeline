"""
PostgreSQL Incremental Ingestion DAG

Airflow DAG that connects to a PostgreSQL source database, reads rows
where updated_at > last_watermark, and writes incrementally to Parquet
on shared storage. Uses XCom for watermark tracking.

Schedule: daily at 2 AM

Environment variables expected:
    POSTGRES_HOST        - PostgreSQL host (default: localhost)
    POSTGRES_PORT        - PostgreSQL port (default: 5432)
    POSTGRES_DB          - Database name (default: source_db)
    POSTGRES_USER        - Database user (default: postgres)
    POSTGRES_PASSWORD    - Database password (default: postgres)
    SHARED_STORAGE_PATH  - Base path for Parquet output (default: /data/landing)
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.utils.dates import days_ago

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
logger = logging.getLogger(__name__)

_DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# Load from environment with defaults
POSTGRES_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "db": os.getenv("POSTGRES_DB", "source_db"),
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", ""),
}

SHARED_STORAGE_PATH = Path(os.getenv("SHARED_STORAGE_PATH", "/data/landing"))
TABLE_NAME = "orders"
WATERMARK_VARIABLE_KEY = f"watermark_{TABLE_NAME}"  # Airflow Variable name


def _get_connection_string() -> str:
    """Build a SQLAlchemy connection string from config."""
    return (
        f"postgresql+psycopg2://{POSTGRES_CONFIG['user']}:"
        f"{POSTGRES_CONFIG['password']}@{POSTGRES_CONFIG['host']}:"
        f"{POSTGRES_CONFIG['port']}/{POSTGRES_CONFIG['db']}"
    )


def _get_engine() -> Engine:
    """Create a SQLAlchemy engine with connection pooling."""
    return create_engine(
        _get_connection_string(),
        pool_size=5,
        max_overflow=10,
        pool_pre_ping=True,
        pool_recycle=3600,
    )


def _get_last_watermark() -> datetime:
    """Retrieve the last ingestion watermark from Airflow Variables.

    Returns:
        The last watermark datetime, or epoch start if none exists.
    """
    watermark_str = Variable.get(WATERMARK_VARIABLE_KEY, default_var=None)
    if watermark_str is None:
        logger.info("No existing watermark found; using epoch start")
        return datetime(1970, 1, 1, tzinfo=None)

    try:
        watermark = datetime.fromisoformat(watermark_str)
        logger.info("Retrieved last watermark: %s", watermark.isoformat())
        return watermark
    except (ValueError, TypeError) as exc:
        logger.warning("Failed to parse watermark '%s': %s; using epoch start", watermark_str, exc)
        return datetime(1970, 1, 1, tzinfo=None)


def _set_last_watermark(watermark: datetime) -> None:
    """Persist the watermark to Airflow Variables.

    Args:
        watermark: The new watermark datetime to store.
    """
    watermark_str = watermark.isoformat()
    Variable.set(WATERMARK_VARIABLE_KEY, watermark_str)
    logger.info("Updated watermark to: %s", watermark_str)


def _get_max_updated_at(engine: Engine, records: pd.DataFrame) -> datetime | None:
    """Extract the maximum updated_at from the ingested records.

    Args:
        engine: SQLAlchemy engine (unused, kept for interface consistency).
        records: DataFrame containing the ingested rows.

    Returns:
        The maximum updated_at value, or None if records is empty.
    """
    if records.empty:
        return None
    return records["updated_at"].max()


# ---------------------------------------------------------------------------
# DAG Definition
# ---------------------------------------------------------------------------
dag = DAG(
    dag_id="postgres_incremental_ingestion",
    default_args=_DEFAULT_ARGS,
    description="Incrementally ingest PostgreSQL tables to Parquet using watermark tracking",
    schedule_interval="0 2 * * *",  # Daily at 2 AM
    start_date=days_ago(1),
    catchup=False,
    tags=["ingestion", "postgres", "incremental"],
    max_active_runs=1,  # Prevent concurrent runs that could corrupt watermarks
)


@task(dag=dag)
def extract_incremental(**context: Any) -> dict[str, Any]:
    """Extract rows from PostgreSQL where updated_at > last watermark.

    Returns:
        A dict containing the file path, row count, and new watermark.
    """
    watermark = _get_last_watermark()
    logger.info(
        "Starting incremental extraction for table '%s' since %s",
        TABLE_NAME,
        watermark.isoformat(),
    )

    engine = _get_engine()
    query = text(
        """
        SELECT *
        FROM {table}
        WHERE updated_at > :watermark
        ORDER BY updated_at ASC
        LIMIT 100000
    """.format(table=TABLE_NAME)
    )

    try:
        with engine.connect() as conn:
            df = pd.read_sql_query(
                sql=query,
                con=conn,
                params={"watermark": watermark},
                parse_dates=["updated_at", "created_at"],
            )
    except Exception as exc:
        logger.error("Failed to extract data from PostgreSQL: %s", exc)
        raise

    if df.empty:
        logger.info("No new records found since watermark %s", watermark.isoformat())
        return {"file_path": None, "row_count": 0, "watermark": watermark.isoformat()}

    logger.info("Extracted %d rows from PostgreSQL", len(df))

    # Determine new watermark
    new_watermark = _get_max_updated_at(engine, df)
    if new_watermark is None:
        new_watermark = watermark

    # Write to Parquet on shared storage
    execution_date = context["ds"]  # Airflow execution date (YYYY-MM-DD)
    output_dir = SHARED_STORAGE_PATH / "postgres" / TABLE_NAME / execution_date
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / f"{TABLE_NAME}_{context['ts_nodash']}.parquet"
    df.to_parquet(str(output_path), index=False, compression="snappy")
    logger.info("Wrote %d rows to %s", len(df), output_path)

    # Persist watermark
    _set_last_watermark(new_watermark)

    return {
        "file_path": str(output_path),
        "row_count": len(df),
        "watermark": new_watermark.isoformat(),
    }


@task(dag=dag)
def validate_ingestion(result: dict[str, Any]) -> None:
    """Validate the ingestion results and log summary.

    Args:
        result: The output dict from extract_incremental task.
    """
    row_count = result.get("row_count", 0)
    file_path = result.get("file_path")

    if row_count == 0 or file_path is None:
        logger.info("No data to validate - ingestion was empty")
        return

    # Verify the file exists and is valid Parquet
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"Expected Parquet file not found: {file_path}")

    file_size = path.stat().st_size
    logger.info(
        "Validation passed: %d rows, %d bytes at %s",
        row_count,
        file_size,
        file_path,
    )
    logger.info("New watermark: %s", result.get("watermark"))


# ---------------------------------------------------------------------------
# Task Pipeline
# ---------------------------------------------------------------------------
result_data = extract_incremental()
validate_ingestion(result_data)
