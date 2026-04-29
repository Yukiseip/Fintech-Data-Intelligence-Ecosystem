"""End-to-End Pipeline Integration Tests.

Requires: All Docker services running (`make up && make generate`).
Run:      pytest tests/integration/ -v --run-integration -m integration

These tests validate the full data flow:
Raw S3 → Bronze → Silver → Gold → alerts_log
"""

from __future__ import annotations
import os
import psycopg2
import pytest

import boto3
from botocore.config import Config


# ── Fixtures ──────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def s3_client():
    """S3 client pointing at LocalStack."""
    return boto3.client(
        "s3",
        endpoint_url=os.environ.get("LOCALSTACK_ENDPOINT", "http://localhost:4566"),
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
        config=Config(signature_version="s3v4"),
    )


@pytest.fixture(scope="module")
def pg_conn():
    """PostgreSQL connection to the Gold DWH."""
    conn = psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        dbname=os.environ.get("POSTGRES_DB", "fintech_dw"),
        user=os.environ.get("POSTGRES_USER", "fintech_user"),
        password=os.environ.get("POSTGRES_PASSWORD", "CHANGE_ME_BEFORE_USE"),
    )
    yield conn
    conn.close()


# ── Tests ─────────────────────────────────────────────────────────────

@pytest.mark.integration
def test_s3_bucket_exists(s3_client):
    """S3 bucket 'fintech-raw-data' must exist after `make infra`."""
    response = s3_client.list_buckets()
    bucket_names = [b["Name"] for b in response.get("Buckets", [])]
    assert "fintech-raw-data" in bucket_names, (
        "Bucket not found. Run `make infra` first."
    )


@pytest.mark.integration
def test_raw_files_exist_after_generation(s3_client):
    """After `make generate`, raw/ prefix must contain at least one file."""
    paginator = s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket="fintech-raw-data", Prefix="raw/transactions/")
    files = [obj for page in pages for obj in page.get("Contents", [])]
    assert len(files) > 0, "No raw transaction files found. Run `make generate`."


@pytest.mark.integration
def test_success_marker_exists(s3_client):
    """A _SUCCESS marker must exist in the raw/ prefix after generation."""
    paginator = s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket="fintech-raw-data", Prefix="raw/transactions/")
    files = [obj["Key"] for page in pages for obj in page.get("Contents", [])]
    success_files = [f for f in files if f.endswith("_SUCCESS")]
    assert len(success_files) > 0, "No _SUCCESS marker found in raw/transactions/"


@pytest.mark.integration
def test_gold_schemas_exist(pg_conn):
    """PostgreSQL must have gold, staging, and bronze schemas."""
    cur = pg_conn.cursor()
    cur.execute("""
        SELECT schema_name FROM information_schema.schemata
        WHERE schema_name IN ('gold', 'staging', 'bronze', 'silver')
    """)
    schemas = {row[0] for row in cur.fetchall()}
    for expected in ("gold", "staging"):
        assert expected in schemas, f"Schema '{expected}' not found in PostgreSQL."


@pytest.mark.integration
def test_dim_time_populated(pg_conn):
    """dim_time must have > 3000 rows (2020-2030 range = ~3650 days)."""
    cur = pg_conn.cursor()
    cur.execute("SELECT COUNT(*) FROM gold.dim_time")
    count = cur.fetchone()[0]
    assert count >= 3000, f"dim_time has only {count} rows — expected >= 3000."


@pytest.mark.integration
def test_fact_transactions_populated_after_pipeline(pg_conn):
    """fact_transactions must have rows after a pipeline run."""
    cur = pg_conn.cursor()
    cur.execute("SELECT COUNT(*) FROM gold.fact_transactions")
    count = cur.fetchone()[0]
    assert count > 0, (
        "fact_transactions is empty. Run `make generate` and trigger the Airflow DAG."
    )


@pytest.mark.integration
def test_silver_rejects_pii_fields(pg_conn):
    """Gold fact_transactions must NOT contain ip_address column."""
    cur = pg_conn.cursor()
    cur.execute("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'gold'
          AND table_name = 'fact_transactions'
          AND column_name = 'ip_address'
    """)
    row = cur.fetchone()
    assert row is None, "ip_address column found in fact_transactions — PII leak!"


@pytest.mark.integration
def test_alerts_log_has_entries(pg_conn):
    """alerts_log must contain at least one entry per fraud rule after pipeline run."""
    cur = pg_conn.cursor()
    cur.execute("""
        SELECT reason_code, COUNT(*)
        FROM gold.alerts_log
        GROUP BY reason_code
    """)
    rules = {row[0] for row in cur.fetchall()}
    expected_rules = {"VELOCITY_ATTACK", "HIGH_AMOUNT", "GEO_IMPOSSIBLE"}
    missing = expected_rules - rules
    assert not missing, (
        f"Missing fraud rules in alerts_log: {missing}. "
        "Ensure anomaly_detection Spark job ran successfully."
    )


@pytest.mark.integration
def test_no_negative_amounts_in_gold(pg_conn):
    """All Gold fact_transactions must have positive amounts."""
    cur = pg_conn.cursor()
    cur.execute("""
        SELECT COUNT(*) FROM gold.fact_transactions WHERE amount <= 0
    """)
    count = cur.fetchone()[0]
    assert count == 0, f"{count} transactions with amount <= 0 found in Gold layer!"
