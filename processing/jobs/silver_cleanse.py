"""
Silver Cleansing Job.

Transforms Bronze Parquet → Silver Parquet with:
  - JSON parsing of raw_data column
  - Type casting and validation
  - Deduplication by transaction_id (keep latest ingestion)
  - PII hashing: ip_address → ip_address_hash (SHA-256)
  - Data quality enforcement (invalid rows rejected & counted)
  - Audit trail via AuditLogger

Usage:
    spark-submit jobs/silver_cleanse.py --date 2026-04-24
"""

from __future__ import annotations
import argparse
import hashlib
import logging
from datetime import datetime, timezone

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import DecimalType, TimestampType
from pyspark.sql.window import Window

from processing.utils.spark_session import get_spark_session
from processing.utils.audit_logger import AuditLogger
from processing.schemas.silver_schema import SILVER_SCHEMA
from processing.schemas.bronze_schema import RAW_TRANSACTION_SCHEMA

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)

VALID_CURRENCIES = ["USD", "EUR", "GBP", "JPY", "CAD", "MXN", "BRL"]
VALID_TXN_TYPES = ["purchase", "refund", "transfer", "withdrawal"]
VALID_STATUSES = ["completed", "pending", "failed", "disputed"]
VALID_METHODS = ["card", "bank_transfer", "wallet", "crypto"]


# ── Step 1: Parse JSON from Bronze raw_data ────────────────────────

def parse_raw_json(df: DataFrame) -> DataFrame:
    """Parse raw_data JSON string into typed columns.

    Args:
        df: Bronze DataFrame with a 'raw_data' string column containing JSON.

    Returns:
        DataFrame with flat, typed columns extracted from JSON payload.
    """
    return df.select(
        F.from_json(F.col("raw_data"), RAW_TRANSACTION_SCHEMA).alias("data"),
        F.col("ingestion_timestamp"),
        F.col("source_file"),
        F.col("partition_date"),
        F.col("bronze_batch_id"),
    ).select("data.*", "ingestion_timestamp", "source_file", "partition_date")


# ── Step 2: Type casting, PII hashing, derived columns ────────────

def cast_and_validate(df: DataFrame) -> DataFrame:
    """Apply type casts, PII hashing, and filter invalid rows.

    Transformations applied:
    - amount → DecimalType(18, 2)
    - timestamp → TimestampType
    - latitude/longitude → DecimalType(9, 6)
    - ip_address → SHA-256 hash (ip_address_hash), original dropped
    - Adds _silver_processed_at audit column

    Rows dropped if:
    - transaction_id, user_id, merchant_id, amount, or timestamp is NULL
    - amount <= 0
    - timestamp is in the future
    - currency / transaction_type / status / payment_method not in valid sets
    - latitude not in [-90, 90] or longitude not in [-180, 180]

    Args:
        df: Parsed DataFrame from parse_raw_json().

    Returns:
        Validated and transformed DataFrame.
    """
    df = (
        df
        # Type casts
        .withColumn("amount",    F.col("amount").cast(DecimalType(18, 2)))
        .withColumn("timestamp", F.col("timestamp").cast(TimestampType()))
        .withColumn("latitude",  F.col("latitude").cast(DecimalType(9, 6)))
        .withColumn("longitude", F.col("longitude").cast(DecimalType(9, 6)))
        # PII hashing: replace ip_address with its SHA-256 hash
        .withColumn("ip_address_hash", F.sha2(F.col("ip_address").cast("binary"), 256))
        .drop("ip_address")
        # Audit column
        .withColumn("_silver_processed_at", F.current_timestamp())
    )

    # Drop rows that fail quality checks
    df = df.filter(
        F.col("transaction_id").isNotNull() &
        F.col("user_id").isNotNull() &
        F.col("merchant_id").isNotNull() &
        F.col("amount").isNotNull() &
        (F.col("amount") > 0) &
        F.col("timestamp").isNotNull() &
        (F.col("timestamp") <= F.current_timestamp()) &   # no future-dated txns
        F.col("currency").isin(VALID_CURRENCIES) &
        F.col("transaction_type").isin(VALID_TXN_TYPES) &
        F.col("status").isin(VALID_STATUSES) &
        F.col("payment_method").isin(VALID_METHODS) &
        # Null-safe geo check: allow NULL coords, reject only known out-of-range
        (
            F.col("latitude").isNull() |
            F.col("latitude").between(-90, 90)
        ) &
        (
            F.col("longitude").isNull() |
            F.col("longitude").between(-180, 180)
        )
    )

    return df


# ── Step 3: Deduplication ──────────────────────────────────────────

def deduplicate(df: DataFrame) -> DataFrame:
    """Keep the latest record per transaction_id (by ingestion_timestamp).

    Uses a row_number window function so it works correctly in distributed
    mode without requiring collect().

    Args:
        df: Validated DataFrame after cast_and_validate().

    Returns:
        DataFrame with at most one row per transaction_id (the latest).
    """
    window = Window.partitionBy("transaction_id").orderBy(
        F.col("ingestion_timestamp").desc()
    )
    return (
        df
        .withColumn("_row_num", F.row_number().over(window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )


# ── Main orchestrator ──────────────────────────────────────────────

def run_silver_cleanse(date: str, s3_bucket: str) -> dict:
    """Execute Silver cleansing pipeline for a given partition date.

    Reads Bronze Parquet from S3, applies parsing → validation → dedup,
    and writes Silver Parquet partitioned by partition_date.

    Args:
        date: Processing date in YYYY-MM-DD format.
        s3_bucket: S3 bucket name (e.g., 'fintech-raw-data').

    Returns:
        dict with keys: input_rows, output_rows, rejected_rows, duration_seconds.
    """
    audit = AuditLogger("silver_cleanse")
    start_time = datetime.now(timezone.utc)

    spark = get_spark_session("SilverCleanse")

    base_path = f"s3a://{s3_bucket}/bronze/transactions/"
    if date.lower() == "all":
        input_path = base_path
    else:
        input_path = f"s3a://{s3_bucket}/bronze/transactions/partition_date={date}/"
        
    output_path = f"s3a://{s3_bucket}/silver/transactions/"

    try:
        raw_df = spark.read.option("basePath", base_path).parquet(input_path)
    except Exception as e:
        if "Path does not exist" in str(e):
            logger.warning("No Bronze data found for date=%s. Exiting.", date)
            spark.stop()
            return {"input_rows": 0, "output_rows": 0, "rejected_rows": 0, "duration_seconds": 0}
        raise e
        
    input_rows = raw_df.count()
    logger.info("Silver input rows: %d", input_rows)

    if input_rows == 0:
        logger.warning("No Bronze data found for date=%s. Exiting.", date)
        spark.stop()
        return {"input_rows": 0, "output_rows": 0, "rejected_rows": 0, "duration_seconds": 0}

    parsed_df = parse_raw_json(raw_df)
    validated_df = cast_and_validate(parsed_df)
    deduped_df = deduplicate(validated_df)

    (
        deduped_df.write
        .mode("append")
        .partitionBy("partition_date")
        .parquet(output_path)
    )

    output_rows = deduped_df.count()
    rejected_rows = input_rows - output_rows
    duration = (datetime.now(timezone.utc) - start_time).total_seconds()

    rejection_pct = 100.0 * rejected_rows / input_rows if input_rows else 0.0
    logger.info(
        "Silver complete. Input: %d, Output: %d, Rejected: %d (%.1f%%). Duration: %.1fs",
        input_rows, output_rows, rejected_rows, rejection_pct, duration,
    )

    audit.log(
        run_id=f"silver-{date}",
        input_rows=input_rows,
        output_rows=output_rows,
        rejected_rows=rejected_rows,
        rejection_pct=round(rejection_pct, 2),
        duration_seconds=round(duration, 2),
    )

    spark.stop()
    return {
        "input_rows": input_rows,
        "output_rows": output_rows,
        "rejected_rows": rejected_rows,
        "duration_seconds": duration,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Silver Cleansing Spark Job")
    parser.add_argument("--date",   required=True, help="Processing date YYYY-MM-DD")
    parser.add_argument("--bucket", default="fintech-raw-data", help="S3 bucket name")
    args = parser.parse_args()

    result = run_silver_cleanse(args.date, args.bucket)
    logger.info("Result: %s", result)
