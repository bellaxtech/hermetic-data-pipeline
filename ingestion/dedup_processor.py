"""
Deduplication Processor

A deduplication processor that takes raw ingested data and removes duplicates
based on a primary key. Implements idempotent insert logic (upsert pattern)
and writes deduped data to Parquet files.

Features:
    - Configurable primary key(s) for deduplication
    - Last-write-wins (LWW) conflict resolution based on timestamp column
    - Upsert semantics: update if exists, insert if new
    - Parquet output with Snappy compression
    - Partitioned output support
    - Detailed dedup statistics reporting

Usage:
    from ingestion.dedup_processor import DedupProcessor, DedupConfig

    config = DedupConfig(
        primary_keys=["order_id"],
        timestamp_column="updated_at",
        output_path="/data/deduped",
    )

    processor = DedupProcessor(config)
    result = processor.process(raw_data_df)

Requirements:
    pyspark >= 3.4  (or pandas >= 2.0 for the pandas backend)
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

import pandas as pd

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
@dataclass
class DedupConfig:
    """Configuration for deduplication processing.

    Attributes:
        primary_keys: List of column names that form the primary key.
        timestamp_column: Column used for conflict resolution (latest wins).
        output_path: Base path for writing deduped Parquet files.
        partition_columns: Columns to partition output by (default: None).
        dedup_strategy: Conflict resolution strategy.
            - 'last_write_wins': Keep the row with the latest timestamp.
            - 'first_write_wins': Keep the first occurrence.
            - 'combine': Merge fields from duplicates (not yet implemented).
        output_file_prefix: Prefix for output Parquet filenames.
        validate_schema: If True, validates schema consistency before processing.
    """

    primary_keys: list[str]
    timestamp_column: str = "updated_at"
    output_path: str = "/data/deduped"
    partition_columns: list[str] | None = None
    dedup_strategy: Literal["last_write_wins", "first_write_wins", "combine"] = (
        "last_write_wins"
    )
    output_file_prefix: str = "deduped"
    validate_schema: bool = True


# ---------------------------------------------------------------------------
# Processing Result
# ---------------------------------------------------------------------------
@dataclass
class DedupResult:
    """Result of a deduplication processing run.

    Attributes:
        input_rows: Total number of rows in the input data.
        unique_rows: Number of unique rows after dedup.
        duplicate_rows: Number of rows removed as duplicates.
        output_path: Path where deduped data was written.
        processing_time_seconds: Time taken for processing.
        processed_at: Timestamp of when processing completed.
    """

    input_rows: int
    unique_rows: int
    duplicate_rows: int
    output_path: str | None
    processing_time_seconds: float
    processed_at: str


# ---------------------------------------------------------------------------
# Dedup Processor
# ---------------------------------------------------------------------------
class DedupProcessor:
    """Deduplication processor implementing idempotent upsert logic.

    This processor handles the "T" in the ELT pipeline — it transforms
    raw (possibly duplicate) data into a clean, deduplicated state ready
    for loading into the data warehouse.
    """

    def __init__(self, config: DedupConfig) -> None:
        """
        Args:
            config: Deduplication configuration.
        """
        self.config = config
        self.logger = logger

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def process(
        self,
        data: pd.DataFrame,
        batch_id: str | None = None,
    ) -> DedupResult:
        """Process raw data through deduplication and write to Parquet.

        This is the main entry point. It removes duplicates based on the
        configured primary keys and writes the clean data to Parquet.

        Args:
            data: Raw DataFrame with potential duplicates.
            batch_id: Optional batch identifier for the output filename.
                If not provided, a timestamp-based ID is generated.

        Returns:
            A DedupResult with processing statistics.

        Raises:
            ValueError: If required columns are missing from the data.
        """
        start_time = time.time()
        input_rows = len(data)

        if input_rows == 0:
            self.logger.warning("Received empty DataFrame, nothing to process")
            return DedupResult(
                input_rows=0,
                unique_rows=0,
                duplicate_rows=0,
                output_path=None,
                processing_time_seconds=0.0,
                processed_at=datetime.now(timezone.utc).isoformat(),
            )

        # Validate input
        self._validate_input(data)

        # Deduplicate
        deduped = self._deduplicate(data)
        unique_rows = len(deduped)
        duplicate_rows = input_rows - unique_rows

        self.logger.info(
            "Deduplication: %d input -> %d unique (%d duplicates removed, %.1f%% reduction)",
            input_rows,
            unique_rows,
            duplicate_rows,
            (duplicate_rows / input_rows * 100) if input_rows > 0 else 0,
        )

        # Write to Parquet
        if batch_id is None:
            batch_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")

        output_path = self._write_parquet(deduped, batch_id)

        elapsed = time.time() - start_time
        self.logger.info(
            "Processing completed in %.2f seconds: %s",
            elapsed,
            output_path,
        )

        return DedupResult(
            input_rows=input_rows,
            unique_rows=unique_rows,
            duplicate_rows=duplicate_rows,
            output_path=output_path,
            processing_time_seconds=round(elapsed, 3),
            processed_at=datetime.now(timezone.utc).isoformat(),
        )

    def process_batch(
        self,
        data_batches: list[pd.DataFrame],
        batch_ids: list[str] | None = None,
    ) -> list[DedupResult]:
        """Process multiple batches and return results.

        Args:
            data_batches: List of DataFrames to process.
            batch_ids: Optional list of batch IDs (must match length of data_batches).

        Returns:
            List of DedupResult, one per batch.
        """
        results: list[DedupResult] = []
        for i, batch in enumerate(data_batches):
            batch_id = batch_ids[i] if batch_ids and i < len(batch_ids) else None
            result = self.process(batch, batch_id=batch_id)
            results.append(result)
        return results

    def upsert_to_existing(
        self,
        new_data: pd.DataFrame,
        existing_data: pd.DataFrame,
        output_path: str | None = None,
    ) -> pd.DataFrame:
        """Merge new data into an existing deduped dataset (upsert).

        Implements idempotent upsert logic:
        - If a row with the same PK exists: compare timestamps, keep newer
        - If a row does not exist: insert it

        Args:
            new_data: Incoming data to merge.
            existing_data: Previously deduped dataset.
            output_path: Optional path to write the merged result.

        Returns:
            Merged DataFrame with dedup applied across both datasets.
        """
        self._validate_input(new_data)
        self._validate_input(existing_data)

        combined = pd.concat([existing_data, new_data], ignore_index=True)
        deduped = self._deduplicate(combined)

        self.logger.info(
            "Upsert: existing=%d, new=%d, merged=%d",
            len(existing_data),
            len(new_data),
            len(deduped),
        )

        if output_path:
            Path(output_path).parent.mkdir(parents=True, exist_ok=True)
            deduped.to_parquet(output_path, index=False, compression="snappy")
            self.logger.info("Upsert result written to: %s", output_path)

        return deduped

    # ------------------------------------------------------------------
    # Internal Methods
    # ------------------------------------------------------------------

    def _validate_input(self, data: pd.DataFrame) -> None:
        """Validate that required columns exist in the data.

        Args:
            data: DataFrame to validate.

        Raises:
            ValueError: If any required column is missing.
        """
        required_columns = set(self.config.primary_keys)
        required_columns.add(self.config.timestamp_column)
        missing = required_columns - set(data.columns)

        if missing:
            raise ValueError(
                f"Missing required columns: {missing}. "
                f"Primary keys: {self.config.primary_keys}, "
                f"Timestamp column: {self.config.timestamp_column}"
            )

        if self.config.validate_schema:
            # Check for nulls in primary key columns
            for pk in self.config.primary_keys:
                null_count = data[pk].isna().sum()
                if null_count > 0:
                    self.logger.warning(
                        "Column '%s' has %d null values in primary key",
                        pk,
                        null_count,
                    )

    def _deduplicate(self, data: pd.DataFrame) -> pd.DataFrame:
        """Perform deduplication based on the configured strategy.

        Args:
            data: DataFrame with potential duplicates.

        Returns:
            DataFrame with duplicates removed.
        """
        if self.config.dedup_strategy == "last_write_wins":
            return self._dedup_last_write_wins(data)
        elif self.config.dedup_strategy == "first_write_wins":
            return self._dedup_first_write_wins(data)
        else:
            raise ValueError(f"Unsupported dedup strategy: {self.config.dedup_strategy}")

    def _dedup_last_write_wins(self, data: pd.DataFrame) -> pd.DataFrame:
        """Remove duplicates keeping the row with the latest timestamp.

        For each group of rows sharing the same primary key values, only
        the row with the most recent timestamp_column value is kept.

        Args:
            data: DataFrame with potential duplicates.

        Returns:
            Deduplicated DataFrame.
        """
        # Sort by timestamp descending so the latest row is first
        sorted_data = data.sort_values(
            by=[*self.config.primary_keys, self.config.timestamp_column],
            ascending=[True] * len(self.config.primary_keys) + [False],
        )

        # Drop duplicates keeping the first (latest timestamp) occurrence
        deduped = sorted_data.drop_duplicates(
            subset=self.config.primary_keys,
            keep="first",
        )

        return deduped.reset_index(drop=True)

    def _dedup_first_write_wins(self, data: pd.DataFrame) -> pd.DataFrame:
        """Remove duplicates keeping the first occurrence.

        Args:
            data: DataFrame with potential duplicates.

        Returns:
            Deduplicated DataFrame.
        """
        return data.drop_duplicates(
            subset=self.config.primary_keys,
            keep="first",
        ).reset_index(drop=True)

    def _write_parquet(self, data: pd.DataFrame, batch_id: str) -> str:
        """Write deduped data to partitioned Parquet files.

        Args:
            data: Deduplicated DataFrame.
            batch_id: Identifier for the output file.

        Returns:
            Path to the written output file or directory.
        """
        base_path = Path(self.config.output_path)
        base_path.mkdir(parents=True, exist_ok=True)

        if self.config.partition_columns:
            # Partitioned write
            output_dir = base_path / f"{self.config.output_file_prefix}_{batch_id}"
            data.to_parquet(
                str(output_dir),
                index=False,
                compression="snappy",
                partition_cols=self.config.partition_columns,
            )
            self.logger.info(
                "Wrote %d rows to partitioned Parquet: %s (partitions: %s)",
                len(data),
                output_dir,
                self.config.partition_columns,
            )
            return str(output_dir)
        else:
            # Single file write
            output_file = base_path / f"{self.config.output_file_prefix}_{batch_id}.snappy.parquet"
            data.to_parquet(str(output_file), index=False, compression="snappy")
            self.logger.info(
                "Wrote %d rows to Parquet: %s",
                len(data),
                output_file,
            )
            return str(output_file)


# ---------------------------------------------------------------------------
# Convenience function
# ---------------------------------------------------------------------------
def dedup_and_write(
    data: pd.DataFrame,
    primary_keys: list[str],
    timestamp_column: str = "updated_at",
    output_path: str = "/data/deduped",
    **kwargs: Any,
) -> DedupResult:
    """Convenience function for one-shot deduplication.

    Args:
        data: Raw DataFrame with potential duplicates.
        primary_keys: Column names forming the primary key.
        timestamp_column: Column for conflict resolution.
        output_path: Output directory for deduped Parquet.
        **kwargs: Additional DedupConfig parameters.

    Returns:
        DedupResult with processing statistics.
    """
    config = DedupConfig(
        primary_keys=primary_keys,
        timestamp_column=timestamp_column,
        output_path=output_path,
        **kwargs,
    )
    processor = DedupProcessor(config)
    return processor.process(data)
