"""
Pytest Fixtures for the Hermetic Data Pipeline Test Suite

Provides shared fixtures and configuration for all test modules.

Fixtures provided:
    - test_data_dir: Temporary directory for test data outputs
    - spark_session: Mock/real SparkSession for Iceberg tests
    - sample_dataframe: A DataFrame with sample pipeline data
    - mock_airflow_context: Simulated Airflow task context
    - async_collector: Configured AsyncDataCollector instance
    - dedup_processor: Configured DedupProcessor instance
    - caplog: Log capture fixture (built-in pytest)
"""

from __future__ import annotations

import logging
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, AsyncGenerator, Generator
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
import pytest_asyncio

from ingestion.async_collector import AsyncDataCollector, CollectorConfig
from ingestion.dedup_processor import DedupConfig, DedupProcessor

# Configure test logging
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
SAMPLE_EVENTS = [
    {"event_id": "evt-001", "user_id": "user-1", "event_type": "click", "value": 1.0, "ts": "2024-01-15T10:00:00Z"},
    {"event_id": "evt-002", "user_id": "user-1", "event_type": "purchase", "value": 49.99, "ts": "2024-01-15T11:00:00Z"},
    {"event_id": "evt-003", "user_id": "user-2", "event_type": "click", "value": 1.0, "ts": "2024-01-16T14:00:00Z"},
]

SAMPLE_ORDERS = [
    {"order_id": "ORD-001", "customer": "Alice", "amount": 150.00, "status": "COMPLETED", "updated_at": "2024-01-15T10:00:00Z"},
    {"order_id": "ORD-002", "customer": "Bob", "amount": 75.50, "status": "PENDING", "updated_at": "2024-01-16T11:00:00Z"},
    {"order_id": "ORD-003", "customer": "Charlie", "amount": 200.00, "status": "COMPLETED", "updated_at": "2024-01-17T09:00:00Z"},
]


# ===========================================================================
# Fixtures
# ===========================================================================

@pytest.fixture(scope="session")
def test_data_dir() -> Generator[Path, None, None]:
    """Create a session-scoped temporary directory for test outputs.

    All test artifacts (Parquet files, logs, etc.) are written here.
    The directory is automatically cleaned up after the test session.
    """
    with tempfile.TemporaryDirectory(prefix="pipeline_test_") as tmpdir:
        logger.info("Test data directory: %s", tmpdir)
        yield Path(tmpdir)


@pytest.fixture(scope="function")
def sample_dataframe() -> pd.DataFrame:
    """Return a DataFrame with sample pipeline data.

    Includes duplicates to test deduplication logic.
    """
    # Include a duplicate with a later timestamp
    data = SAMPLE_EVENTS + [
        {"event_id": "evt-001", "user_id": "user-1", "event_type": "click", "value": 2.0, "ts": "2024-01-15T12:00:00Z"},
    ]
    df = pd.DataFrame(data)
    df["updated_at"] = pd.to_datetime(df["ts"])
    return df


@pytest.fixture(scope="function")
def orders_dataframe() -> pd.DataFrame:
    """Return a DataFrame with sample order data."""
    df = pd.DataFrame(SAMPLE_ORDERS)
    df["updated_at"] = pd.to_datetime(df["updated_at"])
    return df


@pytest.fixture(scope="function")
def mock_airflow_context() -> dict[str, Any]:
    """Simulate an Airflow task context dict.

    This fixture provides a realistic context similar to what Airflow
    passes to @task decorated functions.
    """
    execution_date = datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc)
    return {
        "dag": MagicMock(dag_id="test_dag"),
        "task": MagicMock(task_id="test_task"),
        "execution_date": execution_date,
        "ds": execution_date.strftime("%Y-%m-%d"),
        "ds_nodash": execution_date.strftime("%Y%m%d"),
        "ts": execution_date.isoformat(),
        "ts_nodash": execution_date.strftime("%Y%m%dT%H%M%S"),
        "logical_date": execution_date,
        "run_id": "manual__2024-01-15T10:00:00+00:00",
        "conf": MagicMock(),
        "params": {},
        "dag_run": MagicMock(
            conf={},
            run_id="manual__2024-01-15T10:00:00+00:00",
        ),
        "task_instance": MagicMock(
            key="test_task",
            log_url="http://localhost:8080/log?dag_id=test_dag&task_id=test_task",
            xcom_push=lambda key, value: None,
            xcom_pull=lambda task_ids, key: None,
        ),
        "var": {
            "value": lambda key, default=None: None,
            "json": lambda key, default=None: None,
        },
    }


@pytest.fixture(scope="function")
def async_collector() -> AsyncDataCollector:
    """Provide a configured AsyncDataCollector instance.

    The collector is configured with mock-friendly settings:
    - Small page size for pagination testing
    - Low concurrency for deterministic behavior
    - High rate limit to avoid delays in tests
    """
    config = CollectorConfig(
        base_url="http://test-mock-api",
        endpoint="/test-events",
        api_key="test-api-key",
        page_size=10,
        max_concurrent=2,
        rate_limit=1000.0,
        max_retries=1,
        timeout=5.0,
    )
    return AsyncDataCollector(config)


@pytest.fixture(scope="function")
def dedup_processor(test_data_dir: Path) -> DedupProcessor:
    """Provide a configured DedupProcessor instance.

    Writes test outputs to the session-scoped test_data_dir.
    """
    config = DedupConfig(
        primary_keys=["event_id"],
        timestamp_column="updated_at",
        output_path=str(test_data_dir / "deduped"),
        dedup_strategy="last_write_wins",
        output_file_prefix="test_events",
        validate_schema=True,
    )
    return DedupProcessor(config)


@pytest_asyncio.fixture(scope="function")
async def mock_api_collector(
    async_collector: AsyncDataCollector,
) -> AsyncGenerator[AsyncDataCollector, None]:
    """Provide an async collector with a mocked _fetch_page method.

    The mock returns sample data with pagination support.
    Simulates 2 pages of data.
    """
    pages = [
        {"data": SAMPLE_EVENTS[0:2], "next_page": "2"},
        {"data": SAMPLE_EVENTS[2:], "next_page": None},
    ]
    page_iter = iter(pages)

    async def mock_fetch(params: dict, retry_count: int = 0) -> dict:
        return next(page_iter)

    with patch.object(async_collector, "_fetch_page", side_effect=mock_fetch):
        yield async_collector


# ===========================================================================
# Spark Fixtures (conditionally enabled)
# ===========================================================================

@pytest.fixture(scope="session")
def spark_available() -> bool:
    """Check if a real SparkSession can be created.

    Returns:
        True if PySpark is installed and a local session can be started.
    """
    try:
        import pyspark  # noqa: F401
        return True
    except ImportError:
        logger.warning("PySpark not available; Spark tests will be skipped")
        return False


@pytest.fixture(scope="session")
def spark_session(spark_available: bool) -> Generator[Any, None, None]:
    """Provide a SparkSession for Iceberg tests (if available).

    Skips tests that require Spark if PySpark is not installed.
    This allows the test suite to run in environments without Spark.
    """
    if not spark_available:
        pytest.skip("PySpark not available")
        yield None
        return

    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.appName("TestSuite")
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
        .config("spark.sql.catalog.spark_catalog.type", "hive")
        .config("spark.sql.catalog.spark_catalog.warehouse", "file:///tmp/test_iceberg_warehouse")
        .config("spark.sql.catalog.spark_catalog.cache-enabled", "false")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.shuffle.partitions", "2")
        .master("local[2]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    logger.info("SparkSession created for tests")

    yield spark

    spark.stop()
    logger.info("SparkSession stopped")


# ===========================================================================
# File System Fixtures
# ===========================================================================

@pytest.fixture(scope="function")
def parquet_data_file(test_data_dir: Path, sample_dataframe: pd.DataFrame) -> Path:
    """Write sample data to a Parquet file and return the path."""
    output_path = test_data_dir / "sample_data.parquet"
    sample_dataframe.to_parquet(str(output_path), index=False, compression="snappy")
    logger.info("Wrote sample Parquet data to: %s", output_path)
    return output_path


@pytest.fixture(scope="function")
def empty_parquet_file(test_data_dir: Path) -> Generator[Path, None, None]:
    """Create an empty Parquet file for edge case testing."""
    df = pd.DataFrame()
    output_path = test_data_dir / "empty.parquet"
    df.to_parquet(str(output_path), index=False)
    logger.info("Created empty Parquet file: %s", output_path)
    yield output_path
    if output_path.exists():
        output_path.unlink()


# ===========================================================================
# Environment Fixtures
# ===========================================================================

@pytest.fixture(scope="function", autouse=False)
def set_airflow_env() -> Generator[None, None, None]:
    """Set Airflow-related environment variables for testing.

    Use by adding `set_airflow_env` to the test function parameters.
    These are set per-function and cleaned up after.
    """
    original_env = {}
    test_env = {
        "AIRFLOW_HOME": "/tmp/airflow_test",
        "AIRFLOW__CORE__EXECUTOR": "LocalExecutor",
        "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": "sqlite:////tmp/airflow_test/airflow.db",
        "POSTGRES_HOST": "localhost",
        "POSTGRES_PORT": "5432",
        "POSTGRES_DB": "test_db",
        "POSTGRES_USER": "test_user",
        "POSTGRES_PASSWORD": "test_pass",
        "SHARED_STORAGE_PATH": "/tmp/pipeline_test_data",
    }

    for key, value in test_env.items():
        original_env[key] = os.environ.get(key)
        os.environ[key] = value

    yield

    for key, value in original_env.items():
        if value is None:
            del os.environ[key]
        else:
            os.environ[key] = value


# ===========================================================================
# Custom Assertions
# ===========================================================================

def assert_dataframe_equals(
    actual: pd.DataFrame,
    expected: pd.DataFrame,
    sort_by: str | list[str] | None = None,
    check_dtype: bool = False,
) -> None:
    """Assert that two DataFrames are equal, with optional sorting.

    Args:
        actual: Actual DataFrame.
        expected: Expected DataFrame.
        sort_by: Column(s) to sort by before comparison.
        check_dtype: If True, also check column dtypes match.
    """
    if sort_by:
        actual = actual.sort_values(by=sort_by).reset_index(drop=True)
        expected = expected.sort_values(by=sort_by).reset_index(drop=True)

    pd.testing.assert_frame_equal(actual, expected, check_dtype=check_dtype)


# ===========================================================================
# PyTest Configuration Hooks
# ===========================================================================

def pytest_configure(config: pytest.Config) -> None:
    """Register custom markers and configure pytest."""
    config.addinivalue_line(
        "markers",
        "integration: marks tests as integration tests (not run by default)",
    )
    config.addinivalue_line(
        "markers",
        "spark: marks tests that require a SparkSession",
    )
    config.addinivalue_line(
        "markers",
        "slow: marks tests as slow (>10s)",
    )


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    """Skip slow and integration tests unless explicitly requested.

    Use:
        pytest -v                              # skips integration/slow
        pytest -v -m integration               # only integration tests
        pytest -v -m "not slow"                # all except slow
    """
    for item in items:
        # Skip integration tests by default
        if "integration" in item.keywords:
            item.add_marker(
                pytest.mark.skipif(
                    not item.config.getoption("-m", "").startswith("integration"),
                    reason="Integration test; run with -m integration",
                )
            )
