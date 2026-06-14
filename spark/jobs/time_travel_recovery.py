"""
Iceberg Time Travel and Recovery Demonstration

This PySpark job demonstrates Iceberg's snapshot-based time travel capabilities:
- Write multiple snapshots of data to an Iceberg table
- Query Iceberg metadata (.history, .snapshots) to understand snapshot architecture
- Time travel: read from a specific snapshot ID, read from a timestamp
- Roll back to a previous snapshot

Requirements:
    pyspark >= 3.4
    Apache Iceberg with HiveMetastore catalog

Usage:
    spark-submit \
        --packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.3.1 \
        spark/jobs/time_travel_recovery.py
"""

import logging
import time
from datetime import datetime, timezone
from pyspark.sql import SparkSession

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
CATALOG_NAME = "iceberg_catalog"
NAMESPACE = "analytics"
TABLE_NAME = "time_travel_demo"
FULL_TABLE_NAME = f"{CATALOG_NAME}.{NAMESPACE}.{TABLE_NAME}"


def create_spark_session() -> SparkSession:
    """Create a SparkSession with Iceberg catalog configured for HiveMetastore."""
    spark = (
        SparkSession.builder.appName("IcebergTimeTravel")
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
        .config("spark.sql.catalog.spark_catalog.type", "hadoop")
        .config("spark.sql.catalog.spark_catalog.warehouse", "file:///tmp/iceberg_warehouse")
        .config("spark.sql.catalog.spark_catalog.cache-enabled", "false")
        .config(f"spark.sql.catalog.{CATALOG_NAME}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG_NAME}.type", "hadoop")
        .config(f"spark.sql.catalog.{CATALOG_NAME}.warehouse", "file:///tmp/iceberg_warehouse")
        .config(f"spark.sql.catalog.{CATALOG_NAME}.cache-enabled", "false")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession created with Iceberg HiveMetastore catalog")
    return spark


def create_base_table(spark: SparkSession) -> None:
    """Create the Iceberg table and insert the first snapshot."""
    logger.info("Dropping table if exists: %s", FULL_TABLE_NAME)
    spark.sql(f"DROP TABLE IF EXISTS {FULL_TABLE_NAME}")

    create_sql = f"""
        CREATE TABLE {FULL_TABLE_NAME} (
            id INT,
            product STRING,
            price DECIMAL(10,2),
            category STRING,
            updated_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (category)
        TBLPROPERTIES (
            'format-version'='2',
            'write.metadata.delete-after-commit.enabled'='true',
            'write.metadata.previous-versions-max'='50'
        )
    """
    spark.sql(create_sql)
    logger.info("Created table: %s", FULL_TABLE_NAME)

    # --- Snapshot 1: Initial data ---
    logger.info("--- Snapshot 1: Inserting initial product catalog ---")
    spark.sql(f"""
        INSERT INTO {FULL_TABLE_NAME} VALUES
        (1, 'Laptop', 1299.99, 'Electronics', CAST('2024-01-01 10:00:00' AS TIMESTAMP)),
        (2, 'Mouse', 29.99, 'Electronics', CAST('2024-01-01 10:00:00' AS TIMESTAMP)),
        (3, 'Desk Chair', 399.99, 'Furniture', CAST('2024-01-01 10:00:00' AS TIMESTAMP))
    """)
    _pause("Snapshot 1 committed")

    # --- Snapshot 2: Add more products ---
    logger.info("--- Snapshot 2: Adding more products ---")
    spark.sql(f"""
        INSERT INTO {FULL_TABLE_NAME} VALUES
        (4, 'Monitor', 499.99, 'Electronics', CAST('2024-01-15 12:00:00' AS TIMESTAMP)),
        (5, 'Keyboard', 89.99, 'Electronics', CAST('2024-01-15 12:00:00' AS TIMESTAMP)),
        (6, 'Bookshelf', 249.99, 'Furniture', CAST('2024-01-15 12:00:00' AS TIMESTAMP))
    """)
    _pause("Snapshot 2 committed")

    # --- Snapshot 3: Update some products ---
    logger.info("--- Snapshot 3: Updating prices ---")
    spark.sql(f"""
        UPDATE {FULL_TABLE_NAME} SET price = 1199.99, updated_at = CAST('2024-02-01 09:00:00' AS TIMESTAMP)
        WHERE id = 1
    """)
    spark.sql(f"""
        UPDATE {FULL_TABLE_NAME} SET price = 349.99, updated_at = CAST('2024-02-01 09:00:00' AS TIMESTAMP)
        WHERE id = 3
    """)
    _pause("Snapshot 3 committed")

    # --- Snapshot 4: Delete a product ---
    logger.info("--- Snapshot 4: Deleting a product ---")
    spark.sql(f"DELETE FROM {FULL_TABLE_NAME} WHERE id = 5")
    _pause("Snapshot 4 committed")

    # --- Snapshot 5: Truncate style replacement ---
    logger.info("--- Snapshot 5: Overwriting entire partition (category=Electronics) ---")
    spark.sql(f"""
        INSERT OVERWRITE {FULL_TABLE_NAME} VALUES
        (1, 'Laptop Pro', 1999.99, 'Electronics', CAST('2024-03-01 10:00:00' AS TIMESTAMP)),
        (2, 'Magic Mouse', 79.99, 'Electronics', CAST('2024-03-01 10:00:00' AS TIMESTAMP)),
        (4, 'Ultra Monitor', 799.99, 'Electronics', CAST('2024-03-01 10:00:00' AS TIMESTAMP))
    """)
    _pause("Snapshot 5 committed")


def _pause(label: str) -> None:
    """Log a snapshot completion marker and sleep briefly for distinct timestamps."""
    logger.info("[CHECKPOINT] %s at %s", label, datetime.now(timezone.utc).isoformat())
    time.sleep(1)


def query_metadata(spark: SparkSession) -> None:
    """Query Iceberg metadata to understand the snapshot architecture."""
    logger.info("=== Querying Iceberg Metadata ===")

    # Snapshots metadata
    logger.info("Table snapshots:")
    snapshots = spark.sql(f"SELECT * FROM {FULL_TABLE_NAME}.snapshots")
    snapshots.show(truncate=False)

    # History metadata
    logger.info("Table history:")
    history = spark.sql(f"SELECT * FROM {FULL_TABLE_NAME}.history")
    history.show(truncate=False)

    # Manifests metadata
    logger.info("Table manifests:")
    manifests = spark.sql(f"SELECT * FROM {FULL_TABLE_NAME}.manifests")
    manifests.show(truncate=False)

    # Files metadata (data files)
    logger.info("Table files:")
    files = spark.sql(f"SELECT * FROM {FULL_TABLE_NAME}.files")
    files.show(truncate=False)


def demonstrate_time_travel(spark: SparkSession) -> None:
    """Demonstrate time travel queries using snapshot ID and timestamp."""
    logger.info("=== Demonstrating Time Travel ===")

    # Get current state
    logger.info("Current state of the table:")
    current = spark.sql(f"SELECT * FROM {FULL_TABLE_NAME}")
    current.show(truncate=False)

    # Get snapshot IDs from history
    history_df = spark.sql(
        f"SELECT snapshot_id, made_current_at FROM {FULL_TABLE_NAME}.history ORDER BY made_current_at"
    )
    history_rows = history_df.collect()
    logger.info("Available snapshots:")
    for row in history_rows:
        logger.info("  Snapshot ID: %s, Made current at: %s", row["snapshot_id"], row["made_current_at"])

    if len(history_rows) < 2:
        logger.warning("Not enough snapshots for time travel demonstration")
        return

    # Time travel to the first snapshot (snapshot 1)
    first_snapshot_id = history_rows[0]["snapshot_id"]
    logger.info("=== Time Travel: Reading snapshot ID %s ===", first_snapshot_id)
    time_travel_df = spark.sql(
        f"SELECT * FROM {FULL_TABLE_NAME} VERSION AS OF {first_snapshot_id}"
    )
    logger.info("Data from snapshot %s:", first_snapshot_id)
    time_travel_df.show(truncate=False)

    # Time travel to a specific timestamp (snapshot 2, before updates)
    # Use the timestamp of the second snapshot
    second_snapshot_ts = history_rows[1]["made_current_at"]
    logger.info("=== Time Travel: Reading as of timestamp %s ===", second_snapshot_ts)
    time_travel_ts_df = spark.sql(
        f"SELECT * FROM {FULL_TABLE_NAME} TIMESTAMP AS OF '{second_snapshot_ts}'"
    )
    logger.info("Data as of %s:", second_snapshot_ts)
    time_travel_ts_df.show(truncate=False)

    # Show data from snapshot 3 (mid-way)
    if len(history_rows) >= 4:
        fourth_snapshot_id = history_rows[3]["snapshot_id"]
        logger.info("=== Time Travel: Reading snapshot ID %s ===", fourth_snapshot_id)
        df4 = spark.sql(
            f"SELECT * FROM {FULL_TABLE_NAME} VERSION AS OF {fourth_snapshot_id}"
        )
        logger.info("Data from snapshot %s:", fourth_snapshot_id)
        df4.show(truncate=False)


def demonstrate_rollback(spark: SparkSession) -> None:
    """Roll back the table to a previous snapshot."""
    logger.info("=== Demonstrating Snapshot Rollback ===")

    # Get the second snapshot ID (before updates/deletes)
    history_df = spark.sql(
        f"SELECT snapshot_id, made_current_at FROM {FULL_TABLE_NAME}.history ORDER BY made_current_at"
    )
    history_rows = history_df.collect()

    if len(history_rows) < 2:
        logger.warning("Not enough snapshots for rollback demonstration")
        return

    # Roll back to the second snapshot (Snapshots 1 + 2 only)
    rollback_snapshot_id = history_rows[1]["snapshot_id"]
    logger.info("Rolling back to snapshot ID: %s", rollback_snapshot_id)

    # Use CALL procedure for rollback
    spark.sql(
        f"CALL {CATALOG_NAME}.system.rollback_to_snapshot('{NAMESPACE}.{TABLE_NAME}', {rollback_snapshot_id})"
    )
    logger.info("Rollback completed")

    # Verify the rollback
    logger.info("Data after rollback:")
    after_rollback = spark.sql(f"SELECT * FROM {FULL_TABLE_NAME}")
    after_rollback.show(truncate=False)

    # Verify snapshots again
    logger.info("Snapshots after rollback:")
    snapshots_after = spark.sql(f"SELECT * FROM {FULL_TABLE_NAME}.history")
    snapshots_after.show(truncate=False)

    # Roll forward: restore to the latest snapshot
    if len(history_rows) >= 5:
        latest_snapshot_id = history_rows[-1]["snapshot_id"]
        logger.info("Restoring to latest snapshot ID: %s", latest_snapshot_id)
        try:
            spark.sql(
                f"CALL {CATALOG_NAME}.system.rollback_to_snapshot('{NAMESPACE}.{TABLE_NAME}', {latest_snapshot_id})"
            )
            logger.info("Rollforward completed")

            logger.info("Data after rollforward (should match pre-rollback state):")
            after_restore = spark.sql(f"SELECT * FROM {FULL_TABLE_NAME}")
            after_restore.show(truncate=False)
        except Exception as exc:
            logger.warning("Rollforward not possible (snapshot no longer ancestor): %s", exc)


def main() -> None:
    """Run the Iceberg time travel and recovery demonstration."""
    logger.info("Starting Iceberg Time Travel and Recovery demonstration")
    spark = create_spark_session()

    try:
        create_base_table(spark)
        query_metadata(spark)
        demonstrate_time_travel(spark)
        demonstrate_rollback(spark)
        logger.info("Time Travel demonstration completed successfully")
    except Exception as exc:
        logger.error("Time travel demonstration failed: %s", exc)
        raise
    finally:
        spark.stop()
        logger.info("SparkSession stopped")


if __name__ == "__main__":
    main()
