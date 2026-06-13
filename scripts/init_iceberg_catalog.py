#!/usr/bin/env python3
"""
Iceberg Catalog Initialization Script

Utility script that initializes Iceberg namespaces and creates base tables.
Can be run standalone to bootstrap the catalog for the data pipeline.

This script:
1. Creates the Iceberg namespace (if it doesn't exist)
2. Creates base tables: orders, user_events, product_catalog, audit_log
3. Sets up table properties for optimization and governance
4. Validates the catalog state after initialization

Usage:
    # Standalone
    python scripts/init_iceberg_catalog.py

    # With custom config
    python scripts/init_iceberg_catalog.py --catalog iceberg_catalog --namespace analytics

Requirements:
    pyspark >= 3.4
    Apache Iceberg with HiveMetastore catalog
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from typing import Any

from pyspark.sql import SparkSession

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Default Configuration
# ---------------------------------------------------------------------------
DEFAULT_CATALOG = "iceberg_catalog"
DEFAULT_NAMESPACE = "analytics"
DEFAULT_WAREHOUSE = "file:///tmp/iceberg_warehouse"

# ---------------------------------------------------------------------------
# Table Definitions
#   Each entry defines a base table to create: name, schema DDL, properties.
# ---------------------------------------------------------------------------
TABLE_DEFINITIONS: list[dict[str, Any]] = [
    {
        "name": "orders",
        "description": "Customer order transactions",
        "schema": """
            order_id STRING COMMENT 'Unique order identifier',
            customer_id STRING COMMENT 'Customer identifier',
            product_id STRING COMMENT 'Product identifier',
            quantity INT COMMENT 'Number of units ordered',
            unit_price DECIMAL(18,2) COMMENT 'Price per unit',
            total_amount DECIMAL(18,2) COMMENT 'Total order amount',
            currency STRING COMMENT 'ISO 4217 currency code',
            status STRING COMMENT 'Order status: PENDING, COMPLETED, CANCELLED, REFUNDED',
            shipping_address STRUCT<
                street: STRING,
                city: STRING,
                state: STRING,
                zip: STRING,
                country: STRING
            > COMMENT 'Shipping address',
            created_at TIMESTAMP COMMENT 'Order creation timestamp',
            updated_at TIMESTAMP COMMENT 'Last update timestamp'
        """,
        "partition_by": "days(created_at)",
        "properties": {
            "format-version": "2",
            "write.metadata.delete-after-commit.enabled": "true",
            "write.metadata.previous-versions-max": "25",
            "write.target-file-size-bytes": "134217728",  # 128 MB
            "commit.retry.num-retries": "5",
        },
    },
    {
        "name": "user_events",
        "description": "User interaction events (clickstream, pageviews, etc.)",
        "schema": """
            event_id STRING COMMENT 'Globally unique event identifier',
            user_id STRING COMMENT 'Authenticated user identifier',
            session_id STRING COMMENT 'Browser/app session identifier',
            event_type STRING COMMENT 'Event type: pageview, click, purchase, login, etc.',
            event_name STRING COMMENT 'Specific event name',
            page_url STRING COMMENT 'Page URL where event occurred',
            referrer_url STRING COMMENT 'Referrer URL',
            user_agent STRING COMMENT 'Browser user agent string',
            ip_address STRING COMMENT 'Client IP address',
            metadata MAP<STRING, STRING> COMMENT 'Event-specific key-value metadata',
            event_ts TIMESTAMP COMMENT 'Event timestamp (UTC)',
            ingestion_ts TIMESTAMP COMMENT 'When the event was ingested'
        """,
        "partition_by": "days(event_ts)",
        "properties": {
            "format-version": "2",
            "write.metadata.delete-after-commit.enabled": "true",
            "write.metadata.previous-versions-max": "10",
            "write.target-file-size-bytes": "67108864",  # 64 MB
        },
    },
    {
        "name": "product_catalog",
        "description": "Master product catalog data",
        "schema": """
            product_id STRING COMMENT 'Unique product identifier',
            sku STRING COMMENT 'Stock keeping unit',
            product_name STRING COMMENT 'Product display name',
            description STRING COMMENT 'Product description',
            category STRING COMMENT 'Product category',
            subcategory STRING COMMENT 'Product subcategory',
            brand STRING COMMENT 'Brand/manufacturer name',
            list_price DECIMAL(18,2) COMMENT 'List price',
            cost_price DECIMAL(18,2) COMMENT 'Cost price',
            currency STRING COMMENT 'ISO 4217 currency code',
            is_active BOOLEAN COMMENT 'Whether product is currently active',
            attributes MAP<STRING, STRING> COMMENT 'Product attributes (color, size, etc.)',
            created_at TIMESTAMP COMMENT 'Record creation timestamp',
            updated_at TIMESTAMP COMMENT 'Last update timestamp'
        """,
        "partition_by": "category",
        "properties": {
            "format-version": "2",
            "write.metadata.delete-after-commit.enabled": "true",
            "write.metadata.previous-versions-max": "25",
            "write.target-file-size-bytes": "134217728",  # 128 MB
        },
    },
    {
        "name": "audit_log",
        "description": "System audit log for change data capture and governance",
        "schema": """
            audit_id BIGINT COMMENT 'Auto-incrementing audit identifier',
            table_name STRING COMMENT 'Name of the affected table',
            operation STRING COMMENT 'Operation: INSERT, UPDATE, DELETE',
            record_key STRING COMMENT 'Primary key of the affected record',
            old_value STRING COMMENT 'JSON representation of old values',
            new_value STRING COMMENT 'JSON representation of new values',
            changed_by STRING COMMENT 'User or system that made the change',
            changed_at TIMESTAMP COMMENT 'When the change occurred'
        """,
        "partition_by": "days(changed_at)",
        "properties": {
            "format-version": "2",
            "write.metadata.delete-after-commit.enabled": "true",
            "write.metadata.previous-versions-max": "50",
            "write.target-file-size-bytes": "67108864",  # 64 MB
        },
    },
]


# ---------------------------------------------------------------------------
# Catalog Initializer
# ---------------------------------------------------------------------------
class CatalogInitializer:
    """Initializes an Iceberg catalog with namespaces and base tables."""

    def __init__(
        self,
        catalog_name: str = DEFAULT_CATALOG,
        namespace: str = DEFAULT_NAMESPACE,
        warehouse_path: str = DEFAULT_WAREHOUSE,
        skip_existing: bool = True,
    ) -> None:
        """
        Args:
            catalog_name: Name of the Iceberg catalog.
            namespace: Namespace (database) to create tables in.
            warehouse_path: Warehouse directory for Iceberg tables.
            skip_existing: If True, skip table creation if table exists.
        """
        self.catalog_name = catalog_name
        self.namespace = namespace
        self.warehouse_path = warehouse_path
        self.skip_existing = skip_existing
        self.spark: SparkSession | None = None

    # ------------------------------------------------------------------
    # Spark Session Management
    # ------------------------------------------------------------------

    def _create_spark(self) -> SparkSession:
        """Create a configured SparkSession with Iceberg support."""
        spark = (
            SparkSession.builder.appName("InitIcebergCatalog")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.iceberg.spark.SparkSessionCatalog",
            )
            .config("spark.sql.catalog.spark_catalog.type", "hive")
            .config("spark.sql.catalog.spark_catalog.warehouse", self.warehouse_path)
            .config("spark.sql.catalog.spark_catalog.cache-enabled", "false")
            .config(
                f"spark.sql.catalog.{self.catalog_name}",
                "org.apache.iceberg.spark.SparkCatalog",
            )
            .config(f"spark.sql.catalog.{self.catalog_name}.type", "hive")
            .config(f"spark.sql.catalog.{self.catalog_name}.warehouse", self.warehouse_path)
            .config(f"spark.sql.catalog.{self.catalog_name}.cache-enabled", "false")
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            )
            .master("local[*]")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")
        return spark

    @property
    def _full_table_name(self) -> str:
        """Format the full table path: catalog.namespace.table."""
        return f"{self.catalog_name}.{self.namespace}.{{table_name}}"

    # ------------------------------------------------------------------
    # Initialization Steps
    # ------------------------------------------------------------------

    def create_namespace(self) -> bool:
        """Create the Iceberg namespace (if it doesn't exist).

        Returns:
            True if namespace was created, False if already exists.
        """
        assert self.spark is not None
        try:
            self.spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {self.catalog_name}.{self.namespace}")
            logger.info(
                "Created namespace: %s.%s",
                self.catalog_name,
                self.namespace,
            )
            return True
        except Exception as exc:
            logger.warning(
                "Could not create namespace %s.%s: %s. It may already exist.",
                self.catalog_name,
                self.namespace,
                exc,
            )
            return False

    def create_table(self, table_def: dict[str, Any]) -> bool:
        """Create a single Iceberg table from its definition.

        Args:
            table_def: Table definition dict with name, schema, partition_by, properties.

        Returns:
            True if table was created, False if skipped or failed.
        """
        assert self.spark is not None
        table_name = table_def["name"]
        full_name = self._full_table_name.format(table_name=table_name)

        # Check if table already exists
        if self.skip_existing:
            try:
                self.spark.sql(f"DESCRIBE TABLE {full_name}")
                logger.info("Table already exists, skipping: %s", full_name)
                return False
            except Exception:
                pass  # Table doesn't exist, proceed to create

        # Build CREATE TABLE DDL
        properties_str = ", ".join(
            f"'{k}' = '{v}'" for k, v in table_def.get("properties", {}).items()
        )

        create_sql = f"""
            CREATE TABLE IF NOT EXISTS {full_name} (
                {table_def['schema']}
            )
            USING iceberg
            PARTITIONED BY ({table_def['partition_by']})
            TBLPROPERTIES ({properties_str})
        """

        try:
            self.spark.sql(create_sql)
            logger.info("Created table: %s", full_name)
            return True
        except Exception as exc:
            logger.error("Failed to create table %s: %s", full_name, exc)
            raise

    def show_catalog_state(self) -> None:
        """Display the current state of the catalog."""
        assert self.spark is not None

        logger.info("=== Catalog State ===")

        # Show namespaces
        try:
            namespaces = self.spark.sql(f"SHOW NAMESPACES IN {self.catalog_name}")
            logger.info("Namespaces in %s:", self.catalog_name)
            namespaces.show(truncate=False)
        except Exception as exc:
            logger.warning("Could not list namespaces: %s", exc)

        # Show tables
        try:
            tables = self.spark.sql(
                f"SHOW TABLES IN {self.catalog_name}.{self.namespace}"
            )
            logger.info("Tables in %s.%s:", self.catalog_name, self.namespace)
            tables.show(truncate=False)
        except Exception as exc:
            logger.warning("Could not list tables: %s", exc)

    # ------------------------------------------------------------------
    # Run
    # ------------------------------------------------------------------

    def run(self) -> int:
        """Execute the full catalog initialization.

        Returns:
            Exit code (0 = success, 1 = failure).
        """
        logger.info("Initializing Iceberg catalog: %s", self.catalog_name)
        logger.info("Namespace: %s", self.namespace)
        logger.info("Warehouse: %s", self.warehouse_path)

        self.spark = self._create_spark()

        try:
            # Step 1: Create namespace
            self.create_namespace()

            # Step 2: Create all base tables
            created_count = 0
            skipped_count = 0
            for table_def in TABLE_DEFINITIONS:
                try:
                    if self.create_table(table_def):
                        created_count += 1
                    else:
                        skipped_count += 1
                except Exception as exc:
                    logger.error("Failed to initialize table %s: %s", table_def["name"], exc)
                    raise

            # Step 3: Show final state
            self.show_catalog_state()

            logger.info(
                "Catalog initialization complete: %d tables created, %d skipped",
                created_count,
                skipped_count,
            )
            return 0

        except Exception as exc:
            logger.error("Catalog initialization failed: %s", exc)
            return 1
        finally:
            if self.spark:
                self.spark.stop()


# ---------------------------------------------------------------------------
# CLI Entry Point
# ---------------------------------------------------------------------------
def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Initialize Iceberg catalog with namespaces and base tables",
    )
    parser.add_argument(
        "--catalog",
        default=os.getenv("ICEBERG_CATALOG", DEFAULT_CATALOG),
        help=f"Iceberg catalog name (default: {DEFAULT_CATALOG})",
    )
    parser.add_argument(
        "--namespace",
        default=os.getenv("ICEBERG_NAMESPACE", DEFAULT_NAMESPACE),
        help=f"Iceberg namespace to create (default: {DEFAULT_NAMESPACE})",
    )
    parser.add_argument(
        "--warehouse",
        default=os.getenv("ICEBERG_WAREHOUSE", DEFAULT_WAREHOUSE),
        help=f"Warehouse path (default: {DEFAULT_WAREHOUSE})",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Recreate tables even if they already exist",
    )
    return parser.parse_args()


def main() -> int:
    """Main entry point."""
    args = parse_args()
    logger.info("=" * 60)
    logger.info("Iceberg Catalog Initialization")
    logger.info("=" * 60)

    initializer = CatalogInitializer(
        catalog_name=args.catalog,
        namespace=args.namespace,
        warehouse_path=args.warehouse,
        skip_existing=not args.force,
    )
    return initializer.run()


if __name__ == "__main__":
    sys.exit(main())
