"""
Bronze Ingestion Job.

Reads raw JSONL files from S3 and writes them as Parquet to the Bronze layer.
Adds metadata columns: ingestion_timestamp, source_file, partition_date, bronze_batch_id.

Usage:
    spark-submit jobs/bronze_ingest.py --date 2026-04-24 --hour 15
"""

from __future__ import annotations
import argparse
import logging
import uuid
from datetime import datetime, timezone

from pyspark.sql import functions as F

from processing.utils.audit_logger import AuditLogger
from processing.utils.spark_session import get_spark_session

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)


def run_bronze_ingest(date: str, hour: str, s3_bucket: str) -> dict:
    """Execute Bronze ingestion for a given date/hour partition.

    Reads raw JSONL text from S3 path raw/transactions/{year}/{month}/{day}/{hour}/,
    adds metadata columns, and writes Parquet partitioned by partition_date.

    Args:
        date: Processing date in YYYY-MM-DD format.
        hour: Processing hour in HH format (00-23).
        s3_bucket: Name of the S3 bucket (e.g., 'fintech-raw-data').

    Returns:
        dict with keys: input_rows, output_rows, batch_id, duration_seconds.

    Raises:
        ValueError: If date format is invalid.
    """
    audit = AuditLogger("bronze_ingest")
    start_time = datetime.now(timezone.utc)
    batch_id = str(uuid.uuid4())

    spark = get_spark_session("BronzeIngestion")

    try:
        year, month, day = date.split("-")
    except ValueError as exc:
        raise ValueError(f"date must be YYYY-MM-DD, got '{date}'") from exc

    input_path = f"s3a://{s3_bucket}/raw/transactions/{year}/{month}/{day}/{hour}/"
    output_path = f"s3a://{s3_bucket}/bronze/transactions/"

    logger.info("[%s] Reading from: %s", batch_id, input_path)

    # Read raw JSONL as text lines
    raw_df = (
        spark.read
        .option("multiline", "false")
        .text(input_path)
        .select(
            F.col("value").alias("raw_data"),
            F.input_file_name().alias("source_file"),
        )
        .filter(F.col("raw_data").isNotNull() & (F.length(F.col("raw_data")) > 0))
    )

    input_rows = raw_df.count()
    logger.info("[%s] Input rows: %d", batch_id, input_rows)

    if input_rows == 0:
        logger.warning("[%s] No data found at %s. Exiting.", batch_id, input_path)
        spark.stop()
        return {"input_rows": 0, "output_rows": 0, "batch_id": batch_id, "duration_seconds": 0}

    # Add Bronze metadata columns
    bronze_df = (
        raw_df
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn("partition_date", F.lit(date).cast("date"))
        .withColumn("bronze_batch_id", F.lit(batch_id))
    )

    # Write Parquet, partitioned by partition_date (append-only / immutable)
    (
        bronze_df.write
        .mode("append")
        .partitionBy("partition_date")
        .parquet(output_path)
    )

    output_rows = input_rows  # All rows pass through Bronze (no filtering)
    duration = (datetime.now(timezone.utc) - start_time).total_seconds()

    audit.log(
        run_id=batch_id,
        input_rows=input_rows,
        output_rows=output_rows,
        duration_seconds=duration,
        date=date,
        hour=hour,
        s3_bucket=s3_bucket,
    )

    logger.info(
        "[%s] Bronze complete. Output rows: %d. Duration: %.1fs",
        batch_id, output_rows, duration,
    )
    spark.stop()

    return {
        "input_rows": input_rows,
        "output_rows": output_rows,
        "batch_id": batch_id,
        "duration_seconds": duration,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Bronze Ingestion Job")
    parser.add_argument("--date",   required=True, help="Processing date YYYY-MM-DD")
    parser.add_argument("--hour",   required=True, help="Processing hour HH (00-23)")
    parser.add_argument("--bucket", default="fintech-raw-data", help="S3 bucket name")
    args = parser.parse_args()

    result = run_bronze_ingest(args.date, args.hour, args.bucket)
    logger.info("Result: %s", result)
