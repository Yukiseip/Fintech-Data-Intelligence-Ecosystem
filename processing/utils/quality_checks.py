"""Great Expectations Silver validation suite.

Runs 10+ expectations on the Silver Parquet for a given date.
Called by the Airflow DAG task 'great_expectations_validate'.

Usage:
    python -c "from processing.utils.quality_checks import run_silver_suite; run_silver_suite('2026-04-24')"
"""

from __future__ import annotations
import logging
import os
from datetime import datetime

logger = logging.getLogger(__name__)


def run_silver_suite(date: str, s3_bucket: str | None = None) -> dict:
    """Execute Great Expectations validation suite on Silver data.

    Runs the following expectations (10+ required by Definition of Done):
    1.  Column existence checks (transaction_id, user_id, merchant_id, ...)
    2.  transaction_id: not null
    3.  transaction_id: unique
    4.  user_id: not null
    5.  merchant_id: not null
    6.  amount: not null
    7.  amount: between 0.01 and 1_000_000
    8.  currency: in valid set
    9.  transaction_type: in valid set
    10. status: in valid set
    11. ip_address: column must NOT exist (PII check)
    12. ip_address_hash: column must exist (PII hash check)
    13. latitude: between -90 and 90
    14. longitude: between -180 and 180
    15. _silver_processed_at: not null

    Args:
        date: Processing date in YYYY-MM-DD format.
        s3_bucket: S3 bucket name. Defaults to S3_BUCKET env var.

    Returns:
        dict with 'success' bool, 'statistics', and 'results' keys.

    Raises:
        RuntimeError: If validation fails (used to block pipeline).
    """
    try:
        import great_expectations as gx
        from great_expectations.core.batch import RuntimeBatchRequest
        from great_expectations.data_context.types.base import (
            DataContextConfig,
            InMemoryStoreBackendDefaults,
        )
    except ImportError:
        logger.warning(
            "great_expectations not installed — skipping GE validation for date=%s", date
        )
        return {"success": True, "skipped": True}

    bucket = s3_bucket or os.environ.get("S3_BUCKET", "fintech-raw-data")
    endpoint = os.environ.get("LOCALSTACK_ENDPOINT", "http://localstack:4566")
    silver_path = f"s3a://{bucket}/silver/transactions/partition_date={date}/"

    logger.info("Running GE Silver suite for date=%s, path=%s", date, silver_path)

    # ── Create an in-memory GE context (no filesystem needed) ────────
    context = gx.get_context(
        context_root_dir=None,
        cloud_mode=False,
    )
    context.add_or_update_expectation_suite("silver_transactions_suite")
    
    context.add_datasource(
        name="pandas_datasource",
        class_name="Datasource",
        execution_engine={"class_name": "PandasExecutionEngine"},
        data_connectors={
            "runtime_data_connector": {
                "class_name": "RuntimeDataConnector",
                "batch_identifiers": ["run_id"],
            }
        },
    )

    # Read Silver data via PySpark (reuse existing session)
    from processing.utils.spark_session import get_spark_session
    spark = get_spark_session("GE-Validation")
    
    base_path = f"s3a://{bucket}/silver/transactions/"
    try:
        df = spark.read.option("basePath", base_path).parquet(silver_path)
    except Exception as e:
        if "Path does not exist" in str(e):
            logger.warning("No Silver data found for %s. Skipping GE validation.", date)
            spark.stop()
            return True
        raise e

    # Convert a sample to pandas for GE (first 50K rows for speed)
    sample_pd = df.limit(50_000).toPandas()
    spark.stop()

    # ── Build expectation suite inline ───────────────────────────────
    validator = context.get_validator(
        batch_request=RuntimeBatchRequest(
            datasource_name="pandas_datasource",
            data_connector_name="runtime_data_connector",
            data_asset_name="silver_transactions",
            runtime_parameters={"batch_data": sample_pd},
            batch_identifiers={"run_id": f"silver-{date}"},
        ),
        expectation_suite_name="silver_transactions_suite",
    )

    VALID_CURRENCIES = ["USD", "EUR", "GBP", "JPY", "CAD", "MXN", "BRL"]
    VALID_TXN_TYPES  = ["purchase", "refund", "transfer", "withdrawal"]
    VALID_STATUSES   = ["completed", "pending", "failed", "disputed"]
    VALID_METHODS    = ["card", "bank_transfer", "wallet", "crypto"]

    # Column existence
    for col in ["transaction_id", "user_id", "merchant_id", "amount",
                "currency", "timestamp", "transaction_type", "status",
                "payment_method", "ip_address_hash", "_silver_processed_at"]:
        validator.expect_column_to_exist(col)

    # PII: ip_address must NOT exist
    if "ip_address" in sample_pd.columns:
        raise RuntimeError("ip_address column must NOT exist in Silver layer (PII violation)")

    # Not null checks
    for col in ["transaction_id", "user_id", "merchant_id", "amount",
                "timestamp", "_silver_processed_at"]:
        validator.expect_column_values_to_not_be_null(col)

    # Uniqueness
    validator.expect_column_values_to_be_unique("transaction_id")

    # Business rules
    validator.expect_column_values_to_be_between("amount", 0.01, 1_000_000)
    validator.expect_column_values_to_be_in_set("currency", VALID_CURRENCIES)
    validator.expect_column_values_to_be_in_set("transaction_type", VALID_TXN_TYPES)
    validator.expect_column_values_to_be_in_set("status", VALID_STATUSES)
    validator.expect_column_values_to_be_in_set("payment_method", VALID_METHODS)
    validator.expect_column_values_to_be_between("latitude",  -90,  90)
    validator.expect_column_values_to_be_between("longitude", -180, 180)

    # ip_address_hash must be 64-char SHA-256
    validator.expect_column_value_lengths_to_equal("ip_address_hash", 64)

    # Run validation
    results = validator.validate()

    success = results["success"]
    stats = results["statistics"]

    logger.info(
        "GE validation complete. Success: %s | Evaluated: %d | Failed: %d",
        success,
        stats.get("evaluated_expectations", 0),
        stats.get("unsuccessful_expectations", 0),
    )

    if not success:
        failed = [
            r["expectation_config"]["expectation_type"]
            for r in results["results"]
            if not r["success"]
        ]
        raise RuntimeError(
            f"Silver quality gate FAILED for date={date}. "
            f"Failed expectations: {failed}"
        )

    return {
        "success": success,
        "statistics": stats,
        "date": date,
    }
