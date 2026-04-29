"""
Gold Modeling Job.

Transforms Silver Parquet → PostgreSQL Gold layer (Kimball Star Schema):
  - fact_transactions  (via staging table + upsert)
  - dim_merchants      (via staging table + upsert)
  - dim_users          (staging only; SCD2 merge delegated to dbt snapshot)
  - dim_time           (pre-populated by SQL init script; no write needed)

All credentials are pulled from environment variables — no hardcoding.

Usage:
    spark-submit jobs/gold_modeling.py --date 2026-04-24
"""

from __future__ import annotations
import argparse
import logging
import os
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession, functions as F

from processing.utils.spark_session import get_spark_session
from processing.utils.audit_logger import AuditLogger

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)


# ── JDBC helpers ──────────────────────────────────────────────────────

def get_jdbc_url() -> str:
    """Build PostgreSQL JDBC URL from environment variables.

    Returns:
        JDBC connection URL string.

    Raises:
        KeyError: If POSTGRES_HOST or POSTGRES_DB env vars are not set.
    """
    host = os.environ["POSTGRES_HOST"]
    port = os.environ.get("POSTGRES_PORT", "5432")
    db   = os.environ["POSTGRES_DB"]
    return f"jdbc:postgresql://{host}:{port}/{db}"


def get_jdbc_props() -> dict:
    """Return PostgreSQL JDBC connection properties from env vars.

    Returns:
        Dict with user, password, driver, and batchsize keys.

    Raises:
        KeyError: If POSTGRES_USER or POSTGRES_PASSWORD env vars are not set.
    """
    return {
        "user":      os.environ["POSTGRES_USER"],
        "password":  os.environ["POSTGRES_PASSWORD"],
        "driver":    "org.postgresql.Driver",
        "batchsize": "1000",
    }


# ── Dimension writers ─────────────────────────────────────────────────

def write_staging(df: DataFrame, staging_table: str) -> int:
    """Write DataFrame to a staging table (overwrite).

    Args:
        df: Source Spark DataFrame.
        staging_table: Fully qualified staging table name (e.g., 'staging.dim_users_staging').

    Returns:
        Number of rows written.
    """
    count = df.count()
    df.write.option("truncate", "false").jdbc(
        url=get_jdbc_url(),
        table=staging_table,
        mode="append",
        properties=get_jdbc_props(),
    )
    logger.info("Staged %d rows → %s", count, staging_table)
    return count


# ── Dimension: Merchants ──────────────────────────────────────────────

def write_dim_merchants(silver_df: DataFrame) -> int:
    """Extract distinct merchants from Silver and stage for upsert.

    Args:
        silver_df: Silver Spark DataFrame.

    Returns:
        Number of rows staged.
    """
    merchants_df = silver_df.select(
        F.col("merchant_id"),
        F.col("mcc_code"),
        F.col("country_code"),
    ).distinct()

    return write_staging(merchants_df, "staging.dim_merchants_staging")


# ── Dimension: Users (staging for dbt SCD2) ───────────────────────────

def write_dim_users_staging(silver_df: DataFrame) -> int:
    """Extract distinct users and stage for dbt SCD2 snapshot.

    Note: The SCD2 merge (valid_from / valid_to logic) is handled
    by dbt's snapshot mechanism, not in this Spark job.

    Args:
        silver_df: Silver Spark DataFrame.

    Returns:
        Number of rows staged.
    """
    users_df = silver_df.select(
        F.col("user_id"),
        F.col("country_code"),
        # email_hash and risk_tier come from the Silver layer
        # (populated by the data generator and passed through Silver cleanse)
        F.col("email_hash")  if "email_hash"  in silver_df.columns else F.lit(None).cast("string").alias("email_hash"),
        F.col("risk_tier")   if "risk_tier"   in silver_df.columns else F.lit(None).cast("string").alias("risk_tier"),
    ).dropDuplicates(["user_id"])

    return write_staging(users_df, "staging.dim_users_staging")


# ── Fact: Transactions ────────────────────────────────────────────────

def write_fact_transactions(spark: SparkSession, silver_df: DataFrame) -> int:
    """Transform Silver data into fact_transactions and stage for upsert.

    time_sk is derived from partition_date as YYYYMMDD integer to join
    with the pre-populated dim_time table.

    Args:
        spark: Active SparkSession.
        silver_df: Silver Spark DataFrame.

    Returns:
        Number of rows staged.
    """
    silver_df.createOrReplaceTempView("silver_transactions")

    fact_df = spark.sql("""
        SELECT
            transaction_id,
            user_id,
            merchant_id,
            CAST(DATE_FORMAT(CAST(partition_date AS DATE), 'yyyyMMdd') AS INT) AS time_sk,
            amount,
            currency,
            transaction_type,
            status,
            payment_method,
            FALSE          AS is_flagged_fraud,
            CAST(0.0 AS DECIMAL(5,4)) AS fraud_score,
            latitude,
            longitude,
            country_code
        FROM silver_transactions
    """)

    return write_staging(fact_df, "staging.fact_transactions_staging")


# ── Main orchestrator ──────────────────────────────────────────────────

def run_gold_modeling(date: str, s3_bucket: str) -> dict:
    """Execute Gold modeling pipeline for a given partition date.

    Reads Silver Parquet → stages dim_merchants, dim_users, fact_transactions
    → dbt snapshot + dbt run merge the staging data into Gold tables.

    Args:
        date: Processing date in YYYY-MM-DD format.
        s3_bucket: S3 bucket name (e.g., 'fintech-raw-data').

    Returns:
        dict with output_rows and duration_seconds.
    """
    audit = AuditLogger("gold_modeling")
    start_time = datetime.now(timezone.utc)

    spark = get_spark_session("GoldModeling")

    base_path = f"s3a://{s3_bucket}/silver/transactions/"
    if date.lower() == "all":
        silver_path = base_path
    else:
        silver_path = f"s3a://{s3_bucket}/silver/transactions/partition_date={date}/"
        
    try:
        silver_df = spark.read.option("basePath", base_path).parquet(silver_path)
    except Exception as e:
        if "Path does not exist" in str(e):
            logger.warning("No Silver data path found for date=%s. Exiting.", date)
            spark.stop()
            return {"input_rows": 0, "output_rows": 0, "duration_seconds": 0}
        raise e

    input_rows = silver_df.count()
    logger.info("Gold modeling input rows: %d", input_rows)

    if input_rows == 0:
        logger.warning("No Silver data for date=%s. Exiting.", date)
        spark.stop()
        return {"input_rows": 0, "output_rows": 0, "duration_seconds": 0}

    merchants_rows = write_dim_merchants(silver_df)
    users_rows     = write_dim_users_staging(silver_df)
    fact_rows      = write_fact_transactions(spark, silver_df)

    duration = (datetime.now(timezone.utc) - start_time).total_seconds()

    audit.log(
        run_id=f"gold-{date}",
        input_rows=input_rows,
        output_rows=fact_rows,
        merchants_staged=merchants_rows,
        users_staged=users_rows,
        duration_seconds=round(duration, 2),
    )

    logger.info(
        "Gold modeling complete. Fact: %d, Merchants: %d, Users: %d. Duration: %.1fs",
        fact_rows, merchants_rows, users_rows, duration,
    )

    spark.stop()
    return {
        "input_rows": input_rows,
        "output_rows": fact_rows,
        "duration_seconds": duration,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Gold Modeling Spark Job")
    parser.add_argument("--date",   required=True, help="Processing date YYYY-MM-DD")
    parser.add_argument("--bucket", default="fintech-raw-data", help="S3 bucket name")
    args = parser.parse_args()

    result = run_gold_modeling(args.date, args.bucket)
    logger.info("Result: %s", result)
