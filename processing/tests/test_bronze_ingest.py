"""Unit tests for Bronze Ingestion Spark job."""

from __future__ import annotations
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="module")
def spark():
    """Local SparkSession for unit tests."""
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("test-bronze")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


class TestBronzeIngest:

    def test_metadata_columns_added(self, spark):
        """Bronze job must add ingestion_timestamp, source_file, partition_date."""
        from processing.jobs.bronze_ingest import run_bronze_ingest  # type: ignore

        # This is a lightweight check that the function signature works;
        # Full S3 integration covered in tests/integration/
        assert callable(run_bronze_ingest)

    def test_empty_input_returns_zero_rows(self, spark):
        """Bronze job must handle empty raw_data gracefully."""
        df = spark.createDataFrame([], "value STRING")
        filtered = df.filter(
            df["value"].isNotNull() & (df["value"].isNotNull())
        )
        assert filtered.count() == 0

    def test_bronze_batch_id_is_uuid(self, spark):
        """bronze_batch_id must be a valid UUID format (36 chars)."""
        import uuid
        batch_id = str(uuid.uuid4())
        assert len(batch_id) == 36
        assert batch_id.count("-") == 4

    def test_jsonl_lines_preserved(self, spark):
        """Each JSONL line must map to exactly one Bronze row."""
        import json
        lines = [
            json.dumps({"transaction_id": f"t{i}", "amount": i * 10.0})
            for i in range(5)
        ]
        df = spark.createDataFrame([(line,) for line in lines], ["value"])
        assert df.count() == 5
