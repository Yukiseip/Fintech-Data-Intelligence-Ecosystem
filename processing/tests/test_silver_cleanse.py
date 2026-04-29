"""Unit tests for Silver Cleansing Spark job.

Requires: PySpark + chispa
Run:      pytest processing/tests/test_silver_cleanse.py -v
"""

from __future__ import annotations
import pytest
from datetime import datetime, timezone
from decimal import Decimal

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from chispa import assert_df_equality

from processing.jobs.silver_cleanse import (
    cast_and_validate,
    deduplicate,
)


@pytest.fixture(scope="module")
def spark():
    """Local SparkSession for unit tests."""
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("test-silver")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


# ── Helper ──────────────────────────────────────────────────────────

def make_row(**kwargs) -> dict:
    """Build a valid Silver input row dict, overridable by kwargs."""
    defaults = {
        "transaction_id":   "txn-001",
        "user_id":          "user-001",
        "merchant_id":      "mcc-001",
        "amount":           "100.00",
        "currency":         "USD",
        "timestamp":        "2026-04-24T10:00:00",
        "transaction_type": "purchase",
        "status":           "completed",
        "payment_method":   "card",
        "device_id":        "dev-abc",
        "ip_address":       "192.168.1.1",
        "latitude":         "19.43",
        "longitude":        "-99.13",
        "country_code":     "MX",
        "mcc_code":         "5411",
        "ingestion_timestamp": "2026-04-24T10:01:00",
        "source_file":      "s3://test/file.jsonl",
        "partition_date":   "2026-04-24",
    }
    defaults.update(kwargs)
    return defaults


# ── cast_and_validate tests ──────────────────────────────────────────

class TestCastAndValidate:

    def test_valid_row_passes(self, spark):
        """A fully valid row must not be filtered out."""
        df = spark.createDataFrame([make_row()])
        result = cast_and_validate(df)
        assert result.count() == 1

    def test_null_transaction_id_rejected(self, spark):
        """Rows with NULL transaction_id must be rejected."""
        df = spark.createDataFrame([make_row(transaction_id=None)])
        result = cast_and_validate(df)
        assert result.count() == 0

    def test_null_user_id_rejected(self, spark):
        """Rows with NULL user_id must be rejected."""
        df = spark.createDataFrame([make_row(user_id=None)])
        result = cast_and_validate(df)
        assert result.count() == 0

    def test_negative_amount_rejected(self, spark):
        """Rows with amount <= 0 must be rejected."""
        df = spark.createDataFrame([make_row(amount="-50.00")])
        result = cast_and_validate(df)
        assert result.count() == 0

    def test_zero_amount_rejected(self, spark):
        """Rows with amount = 0 must be rejected."""
        df = spark.createDataFrame([make_row(amount="0.00")])
        result = cast_and_validate(df)
        assert result.count() == 0

    def test_invalid_currency_rejected(self, spark):
        """Rows with unsupported currency must be rejected."""
        df = spark.createDataFrame([make_row(currency="XYZ")])
        result = cast_and_validate(df)
        assert result.count() == 0

    def test_invalid_transaction_type_rejected(self, spark):
        """Rows with unknown transaction_type must be rejected."""
        df = spark.createDataFrame([make_row(transaction_type="bribe")])
        result = cast_and_validate(df)
        assert result.count() == 0

    def test_invalid_latitude_rejected(self, spark):
        """Rows with latitude > 90 must be rejected."""
        df = spark.createDataFrame([make_row(latitude="91.0")])
        result = cast_and_validate(df)
        assert result.count() == 0

    def test_ip_address_column_removed(self, spark):
        """ip_address column must NOT appear in output (PII removal)."""
        df = spark.createDataFrame([make_row()])
        result = cast_and_validate(df)
        assert "ip_address" not in result.columns

    def test_ip_address_hash_added(self, spark):
        """ip_address_hash column must be present in output."""
        df = spark.createDataFrame([make_row()])
        result = cast_and_validate(df)
        assert "ip_address_hash" in result.columns

    def test_ip_hash_is_64_chars_sha256(self, spark):
        """ip_address_hash must be a 64-char hex string (SHA-256)."""
        df = spark.createDataFrame([make_row(ip_address="10.0.0.1")])
        result = cast_and_validate(df)
        row = result.collect()[0]
        assert len(row["ip_address_hash"]) == 64

    def test_silver_processed_at_added(self, spark):
        """_silver_processed_at audit column must be present."""
        df = spark.createDataFrame([make_row()])
        result = cast_and_validate(df)
        assert "_silver_processed_at" in result.columns

    def test_multiple_valid_rows(self, spark):
        """All valid rows in a batch must pass."""
        rows = [make_row(transaction_id=f"txn-{i}") for i in range(10)]
        df = spark.createDataFrame(rows)
        result = cast_and_validate(df)
        assert result.count() == 10

    def test_mixed_valid_and_invalid_rows(self, spark):
        """Only valid rows must pass; invalid rows must be filtered."""
        rows = [
            make_row(transaction_id="txn-good"),
            make_row(transaction_id=None),                  # NULL id
            make_row(transaction_id="txn-neg", amount="-1"),  # negative amount
        ]
        df = spark.createDataFrame(rows)
        result = cast_and_validate(df)
        assert result.count() == 1


# ── deduplicate tests ────────────────────────────────────────────────

class TestDeduplicate:

    def test_deduplicates_by_transaction_id(self, spark):
        """Duplicate transaction_ids must produce exactly one output row."""
        rows = [
            {**make_row(transaction_id="dup-001"), "ingestion_timestamp": "2026-04-24T10:00:00"},
            {**make_row(transaction_id="dup-001"), "ingestion_timestamp": "2026-04-24T10:01:00"},
        ]
        df = spark.createDataFrame(rows)
        result = deduplicate(df)
        assert result.count() == 1

    def test_keeps_latest_ingestion(self, spark):
        """The record with the LATEST ingestion_timestamp must be kept."""
        rows = [
            {**make_row(transaction_id="txn-001", amount="100.00"),
             "ingestion_timestamp": "2026-04-24T10:00:00"},
            {**make_row(transaction_id="txn-001", amount="200.00"),
             "ingestion_timestamp": "2026-04-24T11:00:00"},  # latest
        ]
        df = spark.createDataFrame(rows)
        result = deduplicate(df)
        row = result.collect()[0]
        assert str(row["amount"]) == "200.00"

    def test_unique_rows_unchanged(self, spark):
        """Rows with unique transaction_ids must all pass through."""
        rows = [make_row(transaction_id=f"txn-{i}") for i in range(5)]
        df = spark.createDataFrame(rows)
        result = deduplicate(df)
        assert result.count() == 5
