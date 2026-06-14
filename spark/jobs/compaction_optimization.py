"""
Iceberg Compaction and Optimization Demonstration

This PySpark job demonstrates Iceberg's data compaction capabilities:
- Creates small files intentionally to demonstrate the small files problem
- Uses Iceberg's rewrite_data_files procedure to compact
- Analyzes metadata before/after (file count, total size)

Requirements:
    pyspark >= 3.4
    Apache Iceberg with HiveMetastore catalog

Usage:
    spark-submit \
        --packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.3.1 \
        spark/jobs/compaction_optimization.py
"""

import logging
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, rand, col, round as spark_round

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
TABLE_NAME = "compaction_demo"
FULL_TABLE_NAME = f"{CATALOG_NAME}.{NAMESPACE}.{TABLE_NAME}"


def create_spark_session() -> SparkSession:
    """Create a SparkSession with Iceberg catalog configured for HiveMetastore."""
    spark = (
        SparkSession.builder.appName("IcebergCompaction")
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
        .config("spark.sql.catalog.spark_catalog.type", "hadoop")
        .config("spark.sql.catalog.spark_catalog.warehouse", "file:///tmp/iceberg_warehouse")
        .config("spark.sql.catalog.spark_catalog.cache-enabled", "false")
        .config(f"spark.sql.catalog.{CATALOG_NAME}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG_NAME}.type", "hadoop")
        .config(f"spark.sql.catalog.{CATALOG_NAME}.warehouse", "file:///tmp/iceberg_warehouse")
        .config(f"spark.sql.catalog.{CATALOG_NAME}.cache-enabled", "false")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        # Set small target file size to force many small files
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.files.maxRecordsPerFile", "200")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession created with Iceberg HiveMetastore catalog")
    return spark


def create_and_generate_small_files(spark: SparkSession) -> int:
    """Create an Iceberg table with intentionally many small files.

    Returns:
        The number of data files before compaction.
    """
    logger.info("Dropping table if exists: %s", FULL_TABLE_NAME)
    spark.sql(f"DROP TABLE IF EXISTS {FULL_TABLE_NAME}")

    create_sql = f"""
        CREATE TABLE {FULL_TABLE_NAME} (
            id INT,
            event_ts TIMESTAMP,
            category STRING,
            metric_value DOUBLE,
            description STRING
        )
        USING iceberg
        PARTITIONED BY (category)
        TBLPROPERTIES (
            'format-version'='2',
            'write.metadata.delete-after-commit.enabled'='true',
            'write.target-file-size-bytes'='524288',  -- 512 KB target to create small files
            'write.metadata.previous-versions-max'='50'
        )
    """
    spark.sql(create_sql)
    logger.info("Created Iceberg table with small file target: %s", FULL_TABLE_NAME)

    # Generate 10,000 rows of random data, inserting in batches
    # to create many small files per partition
    num_batches = 20
    rows_per_batch = 500
    categories = ["A", "B", "C", "D"]

    for batch in range(num_batches):
        # Create a batch of random data
        batch_data = []
        for i in range(rows_per_batch):
            row_id = batch * rows_per_batch + i
            category = categories[batch % len(categories)]
            batch_data.append((row_id, category, float(row_id % 100), f"Record_{row_id}_batch_{batch}"))

        # Create dataframe from batch data
        df = spark.createDataFrame(
            batch_data,
            schema=["id", "category", "metric_value", "description"],
        ).withColumn("event_ts", lit("2024-01-15 12:00:00").cast("timestamp"))

        # Write to Iceberg table
        df.writeTo(FULL_TABLE_NAME).append()
        logger.info(
            "Batch %d/%d written (%d rows, category=%s)",
            batch + 1,
            num_batches,
            rows_per_batch,
            categories[batch % len(categories)],
        )

    _pause("All small file batches written")

    # Count total data files before compaction
    file_count_before = spark.sql(
        f"SELECT COUNT(*) as file_count FROM {FULL_TABLE_NAME}.files"
    ).collect()[0]["file_count"]

    total_size_before = spark.sql(
        f"SELECT COALESCE(SUM(file_size_in_bytes), 0) as total_size FROM {FULL_TABLE_NAME}.files"
    ).collect()[0]["total_size"]

    logger.info("=== BEFORE COMPACTION ===")
    logger.info("Data files count: %d", file_count_before)
    logger.info("Total data size: %d bytes (%.2f MB)", total_size_before, total_size_before / (1024 * 1024))

    # Show file distribution per partition
    logger.info("File distribution per partition:")
    file_dist = spark.sql(
        f"""
        SELECT partition.category,
               COUNT(*) as file_count,
               COALESCE(SUM(file_size_in_bytes), 0) as total_size
        FROM {FULL_TABLE_NAME}.files
        GROUP BY partition.category
        ORDER BY partition.category
    """
    )
    file_dist.show(truncate=False)

    return file_count_before


def _pause(label: str) -> None:
    """Log a checkpoint and sleep briefly."""
    logger.info("[CHECKPOINT] %s", label)
    time.sleep(0.5)


def run_compaction(spark: SparkSession) -> None:
    """Run Iceberg's rewrite_data_files procedure to compact small files."""
    logger.info("=== Running Compaction (rewrite_data_files) ===")

    # Run the rewrite procedure
    # Options explained:
    #   - target-file-size-bytes: aim for 64MB files
    #   - min-input-files: minimum files to trigger rewrite
    #   - sort: order within output files for better compression
    compaction_result = spark.sql(
        f"""
        CALL {CATALOG_NAME}.system.rewrite_data_files(
            table => '{NAMESPACE}.{TABLE_NAME}',
            options => map(
                'target-file-size-bytes', '67108864',
                'min-input-files', '5',
                'rewrite-all', 'true'
            )
        )
    """
    )
    logger.info("Compaction procedure completed")
    compaction_result.show(truncate=False)


def analyze_after_compaction(spark: SparkSession) -> None:
    """Analyze metadata after compaction to compare."""
    logger.info("=== AFTER COMPACTION ===")

    file_count_after = spark.sql(
        f"SELECT COUNT(*) as file_count FROM {FULL_TABLE_NAME}.files"
    ).collect()[0]["file_count"]

    total_size_after = spark.sql(
        f"SELECT COALESCE(SUM(file_size_in_bytes), 0) as total_size FROM {FULL_TABLE_NAME}.files"
    ).collect()[0]["total_size"]

    logger.info("Data files count: %d", file_count_after)
    logger.info("Total data size: %d bytes (%.2f MB)", total_size_after, total_size_after / (1024 * 1024))

    # Show file distribution per partition after compaction
    logger.info("File distribution per partition (after compaction):")
    file_dist_after = spark.sql(
        f"""
        SELECT partition.category,
               COUNT(*) as file_count,
               COALESCE(SUM(file_size_in_bytes), 0) as total_size
        FROM {FULL_TABLE_NAME}.files
        GROUP BY partition.category
        ORDER BY partition.category
    """
    )
    file_dist_after.show(truncate=False)

    # Show all files with their sizes
    logger.info("All data files after compaction:")
    files_after = spark.sql(
        f"""
        SELECT file_path,
               file_size_in_bytes,
               record_count,
               partition.category
        FROM {FULL_TABLE_NAME}.files
        ORDER BY file_size_in_bytes DESC
    """
    )
    files_after.show(truncate=False)


def verify_data_integrity(spark: SparkSession) -> None:
    """Verify that data is readable and correct after compaction."""
    logger.info("=== Verifying Data Integrity After Compaction ===")

    total_rows = spark.sql(f"SELECT COUNT(*) as cnt FROM {FULL_TABLE_NAME}").collect()[0]["cnt"]
    logger.info("Total rows: %d", total_rows)

    # Verify aggregate queries work
    agg = spark.sql(
        f"""
        SELECT category,
               COUNT(*) as row_count,
               ROUND(AVG(metric_value), 2) as avg_metric
        FROM {FULL_TABLE_NAME}
        GROUP BY category
        ORDER BY category
    """
    )
    logger.info("Aggregated data after compaction:")
    agg.show(truncate=False)

    # Sample some records
    sample = spark.sql(f"SELECT * FROM {FULL_TABLE_NAME} LIMIT 10")
    logger.info("Sample data after compaction:")
    sample.show(truncate=False)


def main() -> None:
    """Run the Iceberg compaction and optimization demonstration."""
    logger.info("Starting Iceberg Compaction Optimization demonstration")
    spark = create_spark_session()

    try:
        files_before = create_and_generate_small_files(spark)
        run_compaction(spark)
        analyze_after_compaction(spark)
        verify_data_integrity(spark)
        logger.info("Compaction demonstration completed successfully")
    except Exception as exc:
        logger.error("Compaction demonstration failed: %s", exc)
        raise
    finally:
        spark.stop()
        logger.info("SparkSession stopped")


if __name__ == "__main__":
    main()
