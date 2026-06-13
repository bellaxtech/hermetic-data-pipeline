"""
Hive to Iceberg Migration Demonstration

This PySpark job demonstrates migrating a Hive-style external table (Parquet)
to Iceberg using the SNAPSHOT procedure:
- Creates a Hive-style external table with Parquet data
- Migrates it to Iceberg using the snapshot migration
- Validates the migration

Requirements:
    pyspark >= 3.4
    Apache Iceberg with HiveMetastore catalog
    Access to HiveMetastore

Usage:
    spark-submit \
        --packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.3.1 \
        spark/jobs/hive_to_iceberg_migration.py
"""

import logging
import os
import shutil
import tempfile
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
SNAPSHOT_TABLE_NAME = "migrated_legacy_orders"
FULL_SNAPSHOT_TABLE = f"{CATALOG_NAME}.{NAMESPACE}.{SNAPSHOT_TABLE_NAME}"
LEGACY_TABLE_NAME = "legacy_orders"

HIVE_METASTORE_URIS = "thrift://localhost:9083"  # override via env var


def create_spark_session() -> SparkSession:
    """Create a SparkSession with Iceberg catalog and Hive support."""
    spark = (
        SparkSession.builder.appName("HiveToIcebergMigration")
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
        .config("spark.sql.catalog.spark_catalog.type", "hive")
        .config("spark.sql.catalog.spark_catalog.warehouse", "file:///tmp/iceberg_warehouse")
        .config("spark.sql.catalog.spark_catalog.cache-enabled", "false")
        .config(f"spark.sql.catalog.{CATALOG_NAME}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG_NAME}.type", "hive")
        .config(f"spark.sql.catalog.{CATALOG_NAME}.warehouse", "file:///tmp/iceberg_warehouse")
        .config(f"spark.sql.catalog.{CATALOG_NAME}.cache-enabled", "false")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("hive.metastore.uris", os.getenv("HIVE_METASTORE_URIS", HIVE_METASTORE_URIS))
        .config("spark.sql.warehouse.dir", "file:///tmp/iceberg_warehouse")
        .enableHiveSupport()
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession created with Hive support enabled")
    return spark


def create_legacy_hive_table(spark: SparkSession, data_dir: str) -> str:
    """Create a Hive-style external table backed by Parquet data.

    Args:
        spark: Active SparkSession.
        data_dir: Directory for the external table's data.

    Returns:
        The path to the legacy data directory.
    """
    parquet_path = os.path.join(data_dir, "legacy_orders_data")
    os.makedirs(parquet_path, exist_ok=True)
    logger.info("Legacy data directory: %s", parquet_path)

    # Drop existing legacy table
    spark.sql(f"DROP TABLE IF EXISTS {LEGACY_TABLE_NAME}")

    # Create sample Parquet data
    logger.info("Creating sample Parquet data for legacy table...")
    data = [
        (1001, "2024-01-15", "Alice", "Laptop", 1299.99, "COMPLETED"),
        (1002, "2024-01-16", "Bob", "Mouse", 29.99, "COMPLETED"),
        (1003, "2024-01-17", "Charlie", "Keyboard", 89.99, "PENDING"),
        (1004, "2024-01-18", "Diana", "Monitor", 499.99, "COMPLETED"),
        (1005, "2024-01-19", "Eve", "Desk Chair", 399.99, "CANCELLED"),
    ]

    df = spark.createDataFrame(
        data,
        schema=[
            "order_id INT",
            "order_date STRING",
            "customer_name STRING",
            "product STRING",
            "amount DOUBLE",
            "status STRING",
        ],
    )

    # Write as Parquet partitioned by status (mimicking legacy Hive style)
    df.write.mode("overwrite").format("parquet").partitionBy("status").save(parquet_path)
    logger.info("Sample Parquet data written to: %s", parquet_path)

    # Read the Parquet data
    parquet_df = spark.read.format("parquet").load(parquet_path)

    # Create external Hive table on top of the Parquet directory
    create_hive_sql = f"""
        CREATE EXTERNAL TABLE {LEGACY_TABLE_NAME} (
            order_id INT,
            order_date STRING,
            customer_name STRING,
            product STRING,
            amount DOUBLE
        )
        PARTITIONED BY (status STRING)
        STORED AS PARQUET
        LOCATION '{parquet_path}'
    """
    spark.sql(create_hive_sql)
    logger.info("Created Hive external table: %s", LEGACY_TABLE_NAME)
    logger.info("DDL: %s", create_hive_sql)

    # Repair partitions so Hive knows about them
    spark.sql(f"MSCK REPAIR TABLE {LEGACY_TABLE_NAME}")
    logger.info("Partitions repaired for legacy table")

    # Verify legacy data
    legacy_df = spark.sql(f"SELECT * FROM {LEGACY_TABLE_NAME}")
    logger.info("Legacy Hive table contents:")
    legacy_df.show(truncate=False)

    return parquet_path


def migrate_to_iceberg_snapshot(spark: SparkSession) -> None:
    """Migrate the Hive table to Iceberg using the SNAPSHOT procedure.

    The SNAPSHOT procedure creates an Iceberg table that shares the same
    underlying Parquet files with the original Hive table. Both tables
    can coexist and read the same data.
    """
    logger.info("=== Migrating to Iceberg via SNAPSHOT procedure ===")

    # Drop the target table if it already exists
    spark.sql(f"DROP TABLE IF EXISTS {FULL_SNAPSHOT_TABLE}")

    # Use Iceberg's migrate procedure
    # The SNAPSHOT procedure creates an Iceberg table that references the
    # existing Hive table's data files without copying them
    logger.info(
        "Running snapshot migration: %s -> %s",
        LEGACY_TABLE_NAME,
        FULL_SNAPSHOT_TABLE,
    )

    try:
        migrate_result = spark.sql(
            f"CALL {CATALOG_NAME}.system.snapshot('{LEGACY_TABLE_NAME}', '{SNAPSHOT_TABLE_NAME}')"
        )
        logger.info("Migration via SNAPSHOT procedure completed")
        migrate_result.show(truncate=False)
    except Exception as exc:
        logger.warning(
            "SNAPSHOT procedure failed: %s. "
            "This is expected in a non-Hive environment without HMS running. "
            "Falling back to create-table-as-select.",
            exc,
        )
        # Fallback: Create Iceberg table by loading data
        logger.info("=== Fallback: Creating Iceberg table via CTAS ===")
        legacy_df = spark.sql(f"SELECT * FROM {LEGACY_TABLE_NAME}")
        legacy_df.writeTo(FULL_SNAPSHOT_TABLE).using("iceberg").createOrReplace()
        logger.info("Iceberg table created via CTAS fallback")


def validate_migration(spark: SparkSession) -> None:
    """Validate that the migration was successful."""
    logger.info("=== Validating Migration ===")

    # Verify Iceberg table exists and is readable
    try:
        iceberg_df = spark.sql(f"SELECT * FROM {FULL_SNAPSHOT_TABLE}")
        row_count = iceberg_df.count()
        logger.info("Iceberg table row count: %d", row_count)
        logger.info("Iceberg table contents:")
        iceberg_df.show(truncate=False)

        # Verify Iceberg metadata
        snapshots = spark.sql(f"SELECT * FROM {FULL_SNAPSHOT_TABLE}.snapshots")
        logger.info("Iceberg table snapshots:")
        snapshots.show(truncate=False)

        # Verify Iceberg properties
        properties = spark.sql(f"SHOW TBLPROPERTIES {FULL_SNAPSHOT_TABLE}")
        logger.info("Iceberg table properties:")
        properties.show(truncate=False)

        # Check format version
        format_version = spark.sql(
            f"SHOW TBLPROPERTIES {FULL_SNAPSHOT_TABLE} ('format-version')"
        )
        logger.info("Iceberg format version:")
        format_version.show(truncate=False)

        logger.info("Migration validation PASSED")
    except Exception as exc:
        logger.error("Migration validation FAILED: %s", exc)
        raise


def main() -> None:
    """Run the Hive to Iceberg migration demonstration."""
    logger.info("Starting Hive to Iceberg Migration demonstration")
    spark = create_spark_session()

    temp_dir = tempfile.mkdtemp(prefix="hive_iceberg_migration_")
    try:
        create_legacy_hive_table(spark, temp_dir)
        migrate_to_iceberg_snapshot(spark)
        validate_migration(spark)
        logger.info("Migration demonstration completed successfully")
    except Exception as exc:
        logger.error("Migration demonstration failed: %s", exc)
        raise
    finally:
        spark.stop()
        # Clean up temp directory
        shutil.rmtree(temp_dir, ignore_errors=True)
        logger.info("SparkSession stopped and temp directory cleaned")


if __name__ == "__main__":
    main()
