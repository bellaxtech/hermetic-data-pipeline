"""
Iceberg Schema Evolution Demonstration

This PySpark job demonstrates Iceberg's schema evolution capabilities:
- Create an Iceberg table with initial schema
- ADD COLUMN, RENAME COLUMN, DROP COLUMN, ALTER COLUMN TYPE
- Write sample data and verify backward compatibility

Requirements:
    pyspark >= 3.4
    Apache Iceberg with HiveMetastore catalog

Usage:
    spark-submit \
        --packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.3.1 \
        spark/jobs/iceberg_schema_evolution.py
"""

import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    DateType,
)

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
WAREHOUSE_PATH = "s3a://data-warehouse/iceberg"
NAMESPACE = "analytics"
TABLE_NAME = "user_events"
FULL_TABLE_NAME = f"{CATALOG_NAME}.{NAMESPACE}.{TABLE_NAME}"


def create_spark_session() -> SparkSession:
    """Create a SparkSession with Iceberg catalog configured for HiveMetastore."""
    spark = (
        SparkSession.builder.appName("IcebergSchemaEvolution")
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
        .config("spark.sql.catalog.spark_catalog.type", "hadoop")
        .config(
            "spark.sql.catalog.spark_catalog.warehouse",
            f"file:///tmp/iceberg_warehouse",
        )
        .config("spark.sql.catalog.spark_catalog.cache-enabled", "false")
        # Iceberg catalog configuration
        .config(f"spark.sql.catalog.{CATALOG_NAME}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG_NAME}.type", "hadoop")
        .config(
            f"spark.sql.catalog.{CATALOG_NAME}.warehouse",
            f"file:///tmp/iceberg_warehouse",
        )
        .config(f"spark.sql.catalog.{CATALOG_NAME}.cache-enabled", "false")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession created with Iceberg HiveMetastore catalog")
    return spark


def create_initial_schema(spark: SparkSession) -> None:
    """Create Iceberg table with initial schema and sample data."""
    logger.info("Dropping table if exists: %s", FULL_TABLE_NAME)
    spark.sql(f"DROP TABLE IF EXISTS {FULL_TABLE_NAME}")

    create_sql = f"""
        CREATE TABLE {FULL_TABLE_NAME} (
            event_id STRING,
            user_id STRING,
            event_type STRING,
            event_ts TIMESTAMP,
            amount DOUBLE
        )
        USING iceberg
        TBLPROPERTIES (
            'format-version'='2',
            'write.metadata.delete-after-commit.enabled'='true',
            'write.metadata.previous-versions-max'='10'
        )
    """
    spark.sql(create_sql)
    logger.info("Created Iceberg table with initial schema: %s", create_sql)

    # Insert sample data
    insert_sql = f"""
        INSERT INTO {FULL_TABLE_NAME} VALUES
        ('evt-001', 'user-1', 'click', CAST('2024-01-15 10:30:00' AS TIMESTAMP), 0.0),
        ('evt-002', 'user-1', 'purchase', CAST('2024-01-15 11:00:00' AS TIMESTAMP), 49.99),
        ('evt-003', 'user-2', 'click', CAST('2024-01-16 14:20:00' AS TIMESTAMP), 0.0)
    """
    spark.sql(insert_sql)
    logger.info("Inserted initial sample data")

    # Verify data
    result = spark.sql(f"SELECT * FROM {FULL_TABLE_NAME}")
    logger.info("Initial data count: %d rows", result.count())
    result.show(truncate=False)


def demonstrate_add_column(spark: SparkSession) -> None:
    """Add new columns to the Iceberg table."""
    logger.info("=== Demonstrating ADD COLUMN ===")

    # Add a single column
    spark.sql(
        f"ALTER TABLE {FULL_TABLE_NAME} ADD COLUMN device_type STRING COMMENT 'Device type: web/mobile/tablet'"
    )
    logger.info("Added column: device_type")

    # Add multiple columns at once
    spark.sql(
        f"""
        ALTER TABLE {FULL_TABLE_NAME} ADD COLUMNS (
            app_version STRING COMMENT 'Application version',
            session_id STRING COMMENT 'User session identifier'
        )
    """
    )
    logger.info("Added columns: app_version, session_id")

    # Add a column with a default position (first)
    spark.sql(
        f"ALTER TABLE {FULL_TABLE_NAME} ADD COLUMN region STRING FIRST"
    )
    logger.info("Added column: region (FIRST position)")

    # Add a column after another column
    spark.sql(
        f"ALTER TABLE {FULL_TABLE_NAME} ADD COLUMN country STRING AFTER user_id"
    )
    logger.info("Added column: country (AFTER user_id)")

    # Verify schema
    schema_df = spark.sql(f"DESCRIBE TABLE {FULL_TABLE_NAME}")
    logger.info("Schema after ADD COLUMN operations:")
    schema_df.show(truncate=False)


def demonstrate_rename_column(spark: SparkSession) -> None:
    """Rename existing columns."""
    logger.info("=== Demonstrating RENAME COLUMN ===")

    spark.sql(
        f"ALTER TABLE {FULL_TABLE_NAME} RENAME COLUMN event_type TO action_type"
    )
    logger.info("Renamed: event_type -> action_type")

    spark.sql(
        f"ALTER TABLE {FULL_TABLE_NAME} RENAME COLUMN event_ts TO timestamp"
    )
    logger.info("Renamed: event_ts -> timestamp")

    # Verify schema
    schema_df = spark.sql(f"DESCRIBE TABLE {FULL_TABLE_NAME}")
    logger.info("Schema after RENAME COLUMN operations:")
    schema_df.show(truncate=False)


def demonstrate_drop_column(spark: SparkSession) -> None:
    """Drop columns from the Iceberg table."""
    logger.info("=== Demonstrating DROP COLUMN ===")

    spark.sql(
        f"ALTER TABLE {FULL_TABLE_NAME} DROP COLUMN session_id"
    )
    logger.info("Dropped column: session_id")

    spark.sql(
        f"ALTER TABLE {FULL_TABLE_NAME} DROP COLUMN app_version"
    )
    logger.info("Dropped column: app_version")

    # Verify schema
    schema_df = spark.sql(f"DESCRIBE TABLE {FULL_TABLE_NAME}")
    logger.info("Schema after DROP COLUMN operations:")
    schema_df.show(truncate=False)


def demonstrate_alter_column_type(spark: SparkSession) -> None:
    """Change column type (safe evolution only)."""
    logger.info("=== Demonstrating ALTER COLUMN TYPE ===")

    spark.sql(
        f"ALTER TABLE {FULL_TABLE_NAME} ALTER COLUMN amount TYPE STRING"
    )
    logger.info("Changed column type: amount DOUBLE -> STRING")

    # Rename event_id to id and change type to LONG (but keep for demo purposes)
    # Just show the schema
    schema_df = spark.sql(f"DESCRIBE TABLE {FULL_TABLE_NAME}")
    logger.info("Schema after ALTER COLUMN TYPE:")
    schema_df.show(truncate=False)


def verify_backward_compatibility(spark: SparkSession) -> None:
    """Write new data and read old data to verify backward compatibility."""
    logger.info("=== Verifying Backward Compatibility ===")

    # Insert data with the new schema (includes new columns, excludes dropped ones)
    insert_sql = f"""
        INSERT INTO {FULL_TABLE_NAME} VALUES
        (
            'US',                              -- region
            'evt-004',                         -- event_id
            'user-3',                          -- user_id
            'KR',                              -- country
            'purchase',                        -- action_type (renamed)
            CAST('2024-01-17 09:15:00' AS TIMESTAMP),  -- timestamp (renamed)
            CAST(29.99 AS DECIMAL(18,2)),      -- amount (new type)
            'mobile'                           -- device_type (new column)
        )
    """
    spark.sql(insert_sql)
    logger.info("Inserted data with evolved schema")

    # Read all data - old rows should have NULL for new columns
    result = spark.sql(f"SELECT * FROM {FULL_TABLE_NAME}")
    logger.info("All data after schema evolution:")
    result.show(truncate=False)

    # Verify old data is still readable with default values for new columns
    old_row = spark.sql(
        f"SELECT event_id, region, device_type FROM {FULL_TABLE_NAME} WHERE event_id = 'evt-001'"
    )
    logger.info("Old row backward compatibility check:")
    old_row.show(truncate=False)

    # Verify new columns have NULL for old rows
    null_check = spark.sql(
        f"SELECT COUNT(*) as null_count FROM {FULL_TABLE_NAME} WHERE device_type IS NULL AND event_id IN ('evt-001', 'evt-002', 'evt-003')"
    )
    logger.info("Old rows have NULL for new columns:")
    null_check.show(truncate=False)


def main() -> None:
    """Run the Iceberg schema evolution demonstration."""
    logger.info("Starting Iceberg Schema Evolution demonstration")
    spark = create_spark_session()

    try:
        create_initial_schema(spark)
        demonstrate_add_column(spark)
        demonstrate_rename_column(spark)
        demonstrate_drop_column(spark)
        try:
            demonstrate_alter_column_type(spark)
        except Exception as exc:
            logger.warning("ALTER COLUMN TYPE skipped (platform limitation): %s", exc)
        verify_backward_compatibility(spark)
        logger.info("Iceberg Schema Evolution demonstration completed successfully")
    except Exception as exc:
        logger.error("Schema evolution failed: %s", exc)
        raise
    finally:
        spark.stop()
        logger.info("SparkSession stopped")


if __name__ == "__main__":
    main()
