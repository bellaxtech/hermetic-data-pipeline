"""
XCom Optimization DAG

Demonstrates the best practice of passing file paths (not large data objects)
via Airflow XCom. Task 1 processes data and writes to shared storage, returning
only the file path. Task 2 reads the path and loads the data from storage.

This pattern avoids:
- XCom size limits (48KB by default, configurable but not recommended for large data)
- Database bloat from storing serialized objects
- Performance degradation from serialization/deserialization of large payloads

Usage:
    This DAG is designed for Airflow 2.8+. It uses the @task decorator and
    TaskFlow API which support automatic XCom argument mapping.

Environment:
    SHARED_STORAGE_PATH: Base path for data files (default: /data/shared)
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
logger = logging.getLogger(__name__)

_SHARED_STORAGE = Path(os.getenv("SHARED_STORAGE_PATH", "/data/shared"))

_DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

# ---------------------------------------------------------------------------
# DAG Definition
# ---------------------------------------------------------------------------
dag = DAG(
    dag_id="xcom_optimization_demo",
    default_args=_DEFAULT_ARGS,
    description="Demonstrates passing file paths (not data) via XCom for efficient task communication",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["example", "xcom", "optimization"],
)


@task(dag=dag)
def task_a_process_and_write(**context: Any) -> dict[str, Any]:
    """Process data and write results to shared storage.

    Simulates a heavy processing step that produces a large dataset.
    Instead of returning the data itself via XCom, we write to shared
    storage and return only the file path and metadata.

    Returns:
        A dict with:
            - file_path: Path to the written Parquet file
            - record_count: Number of records processed
            - processing_timestamp: When processing completed
    """
    execution_date = context["ds"]
    logger.info("Task A: Processing data for date %s", execution_date)

    # Simulate data processing
    processed_data = [
        {"id": i, "value": i * 1.5, "category": "A" if i % 2 == 0 else "B", "date": execution_date}
        for i in range(100_000)  # 100K rows — would be huge in XCom
    ]

    df = pd.DataFrame(processed_data)
    logger.info("Task A: Generated %d records", len(df))

    # Write to shared storage
    output_dir = _SHARED_STORAGE / "processed" / execution_date
    output_dir.mkdir(parents=True, exist_ok=True)

    file_path = output_dir / f"processed_data_{context['ts_nodash']}.parquet"
    df.to_parquet(str(file_path), index=False, compression="snappy")
    file_size_mb = file_path.stat().st_size / (1024 * 1024)
    logger.info(
        "Task A: Wrote %d records (%.2f MB) to %s",
        len(df),
        file_size_mb,
        file_path,
    )

    # Return ONLY the file path and metadata — NOT the data itself
    result = {
        "file_path": str(file_path),
        "record_count": len(df),
        "processing_timestamp": datetime.utcnow().isoformat(),
    }
    logger.info(
        "Task A: Returning XCom payload of ~%d bytes (just path + metadata, not %d data rows)",
        len(str(result)),
        len(df),
    )
    return result


@task(dag=dag)
def task_b_read_and_transform(metadata: dict[str, Any]) -> dict[str, Any]:
    """Read the file path from XCom and load data from shared storage.

    This task receives ONLY the metadata dict (containing the file path)
    via XCom. It then reads the actual data from the shared filesystem.

    Args:
        metadata: The dict output from task_a_process_and_write containing
            the file_path, record_count, and processing_timestamp.

    Returns:
        A dict with transformation results and summary statistics.
    """
    file_path = metadata.get("file_path")
    expected_count = metadata.get("record_count", 0)

    if not file_path:
        raise ValueError("No file_path found in XCom metadata")

    logger.info("Task B: Reading data from %s", file_path)
    logger.info("Task B: Expected %d records", expected_count)

    # Verify file exists
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"Data file not found: {file_path}")

    # Load from shared storage (not from XCom!)
    df = pd.read_parquet(file_path)
    actual_count = len(df)
    logger.info("Task B: Loaded %d records from Parquet", actual_count)

    # Perform transformation
    summary = df.groupby("category").agg(
        count=("id", "count"),
        sum_value=("value", "sum"),
        avg_value=("value", "mean"),
    ).reset_index()

    logger.info("Task B: Transformation summary:")
    logger.info("\n%s", summary.to_string())

    # Write transformation output
    output_dir = _SHARED_STORAGE / "transformed" / str(path.parent.name)
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"summary_{path.stem}.csv"
    summary.to_csv(str(output_path), index=False)
    logger.info("Task B: Summary written to %s", output_path)

    return {
        "input_file": file_path,
        "records_read": actual_count,
        "records_expected": expected_count,
        "output_file": str(output_path),
        "categories": summary["category"].tolist(),
    }


@task(dag=dag)
def task_c_verify(metadata: dict[str, Any]) -> None:
    """Verify the pipeline results.

    Args:
        metadata: The output dict from task_b_read_and_transform.
    """
    logger.info("=== Pipeline Verification ===")
    logger.info("Input file: %s", metadata.get("input_file"))
    logger.info("Records read: %d (expected: %d)", metadata.get("records_read"), metadata.get("records_expected"))
    logger.info("Output file: %s", metadata.get("output_file"))
    logger.info("Categories processed: %s", metadata.get("categories"))

    if metadata.get("records_read") != metadata.get("records_expected"):
        raise ValueError(
            f"Record count mismatch: read {metadata['records_read']}, "
            f"expected {metadata['records_expected']}"
        )

    # Verify output file exists
    output_path = Path(metadata["output_file"])
    if not output_path.exists():
        raise FileNotFoundError(f"Output file not found: {metadata['output_file']}")

    logger.info("Pipeline verification PASSED")
    logger.info("XCom optimization pattern: data written to shared storage, only paths passed via XCom")


# ---------------------------------------------------------------------------
# Task Pipeline
# ---------------------------------------------------------------------------
a_result = task_a_process_and_write()
b_result = task_b_read_and_transform(a_result)
task_c_verify(b_result)
