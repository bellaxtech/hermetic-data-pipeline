"""
Spark Iceberg MERGE INTO DAG

Airflow DAG that uses SparkSubmitOperator to submit a PySpark job to a Spark
Master. The job performs MERGE INTO on an Iceberg table (upsert logic).
Handles retries and failure callbacks with a Slack webhook mock.

Requirements:
    - Airflow 2.8+
    - apache-airflow-providers-apache-spark
    - Spark cluster accessible at the configured master URL

Environment Variables:
    SPARK_MASTER        - Spark master URL (default: spark://spark-master:7077)
    ICEBERG_WAREHOUSE   - Iceberg warehouse path (default: s3a://data-warehouse/iceberg)
    SPARK_HOME          - Spark installation directory
    SLACK_WEBHOOK_URL   - Slack webhook for failure notifications (optional)
"""

from __future__ import annotations

import json
import logging
import os
from datetime import timedelta
from typing import Any

import requests

from airflow import DAG
from airflow.decorators import task
from airflow.models.baseoperator import chain
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
logger = logging.getLogger(__name__)

_DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,  # We use custom Slack callback instead
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(hours=1),
}

SPARK_MASTER = os.getenv("SPARK_MASTER", "spark://spark-master:7077")
ICEBERG_WAREHOUSE = os.getenv("ICEBERG_WAREHOUSE", "s3a://data-warehouse/iceberg")
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "")

# Path to the PySpark job file on the shared filesystem
# In a cluster setup, this would be on a shared volume accessible to all workers
SPARK_JOB_PATH = "/opt/spark/jobs/iceberg_merge_upsert.py"

# Spark packages for Iceberg
ICEBERG_PACKAGES = (
    "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.3.1,"
    "org.apache.hadoop:hadoop-aws:3.3.4"
)


# ---------------------------------------------------------------------------
# Failure callback
# ---------------------------------------------------------------------------
def _send_slack_notification(context: dict[str, Any]) -> None:
    """Send a failure notification to Slack (or log it as a mock).

    Args:
        context: Airflow task instance context dict.
    """
    dag_id = context["dag"].dag_id
    task_id = context["task"].task_id
    execution_date = str(context["execution_date"])
    log_url = context.get("task_instance").log_url

    message = (
        f":fire: *Airflow Task Failed*\n"
        f"*DAG:* {dag_id}\n"
        f"*Task:* {task_id}\n"
        f"*Execution Date:* {execution_date}\n"
        f"*Logs:* {log_url}\n"
    )

    if SLACK_WEBHOOK_URL:
        try:
            response = requests.post(
                SLACK_WEBHOOK_URL,
                json={"text": message},
                timeout=10,
            )
            response.raise_for_status()
            logger.info("Slack notification sent successfully")
        except requests.RequestException as exc:
            logger.warning("Failed to send Slack notification: %s", exc)
    else:
        # Mock: log the message that would be sent
        logger.info("=== MOCK SLACK NOTIFICATION ===")
        logger.info("Would send to %s:", SLACK_WEBHOOK_URL or "<no webhook configured>")
        logger.info(message)
        logger.info("=== END MOCK SLACK NOTIFICATION ===")


# ---------------------------------------------------------------------------
# DAG Definition
# ---------------------------------------------------------------------------
dag = DAG(
    dag_id="spark_iceberg_merge",
    default_args=_DEFAULT_ARGS,
    description="Submit PySpark job to Spark Master for Iceberg MERGE INTO upsert",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["spark", "iceberg", "merge", "upsert"],
    on_failure_callback=_send_slack_notification,
)


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------
spark_merge_job = SparkSubmitOperator(
    task_id="run_iceberg_merge",
    application=SPARK_JOB_PATH,
    name="IcebergMergeUpsert",
    conn_id="spark_default",  # Airflow connection for Spark
    conf={
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.sql.catalog.iceberg_catalog": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.iceberg_catalog.type": "hive",
        "spark.sql.catalog.iceberg_catalog.warehouse": ICEBERG_WAREHOUSE,
        "spark.sql.catalog.iceberg_catalog.cache-enabled": "false",
        "spark.sql.sources.partitionOverwriteMode": "dynamic",
        "spark.sql.shuffle.partitions": "16",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    },
    jars=os.getenv("SPARK_ICEBERG_JARS", ""),
    packages=os.getenv("SPARK_ICEBERG_PACKAGES", ICEBERG_PACKAGES),
    principal=os.getenv("SPARK_PRINCIPAL", ""),
    keytab=os.getenv("SPARK_KEYTAB", ""),
    driver_memory="4g",
    executor_memory="8g",
    num_executors=4,
    executor_cores=2,
    verbose=True,
    on_failure_callback=_send_slack_notification,
    execution_timeout=timedelta(hours=2),
    dag=dag,
)


@task(dag=dag)
def validate_merge_result(**context: Any) -> None:
    """Validate that the merge job completed by checking logs or metadata.

    In production, this task would query the Iceberg table to verify
    row counts or inspect the Spark job history server.
    """
    logger.info("Validating Iceberg MERGE INTO result...")

    # Log the task context for observability
    logical_date = context["logical_date"]
    logger.info("Execution date: %s", logical_date)

    # In a real pipeline, you might:
    # 1. Query Iceberg table for row counts
    # 2. Check Spark job history server for success
    # 3. Verify target/source row counts match

    logger.info("MERGE INTO validation completed (mock)")


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------
validate_merge_result(spark_merge_job)
