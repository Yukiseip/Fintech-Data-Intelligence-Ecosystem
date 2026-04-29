"""
Fintech Master Pipeline DAG.

Schedule: Hourly (@hourly)
Orchestrates: Data Generation → Bronze → Silver → Gold → dbt → GE → Anomaly Detection

Author: data-engineering
SLA: 45 minutes from DAG start
"""

from __future__ import annotations
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.s3_key_sensor import S3KeySensor
from airflow.utils.task_group import TaskGroup

# ── DAG defaults ──────────────────────────────────────────────────────
DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
    "depends_on_past": False,
}

BUCKET = os.environ.get("S3_BUCKET", "fintech-raw-data")

# Spark configuration — all values from env vars (no hardcoding)
SPARK_CONF = {
    "spark.executor.memory": os.environ.get("SPARK_EXECUTOR_MEMORY", "2g"),
    "spark.executor.cores": os.environ.get("SPARK_EXECUTOR_CORES", "2"),
    "spark.hadoop.fs.s3a.endpoint": os.environ.get("LOCALSTACK_ENDPOINT", "http://localstack:4566"),
    "spark.hadoop.fs.s3a.access.key": "test",
    "spark.hadoop.fs.s3a.secret.key": "test",
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.sql.adaptive.enabled": "true",
}


# ── Main DAG ──────────────────────────────────────────────────────────
with DAG(
    dag_id="fintech_master_pipeline",
    default_args=DEFAULT_ARGS,
    description="Fintech ETL: Raw → Bronze → Silver → Gold → Anomaly Detection",
    schedule_interval="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["fintech", "etl", "critical"],
    doc_md="""
    ## Fintech Master Pipeline

    Hourly pipeline orchestrating the full Medallion Architecture:
    1. **Data Generation** — synthetic transactions uploaded to S3
    2. **Bronze Ingestion** — JSONL → Parquet with metadata
    3. **Silver Cleansing** — dedup + PII hash + validation
    4. **Gold Modeling** — star schema → PostgreSQL
    5. **Quality & dbt** — Great Expectations + dbt tests
    6. **Anomaly Detection** — 3 fraud rules + alerts
    """,
) as dag:

    # ──────────────────────────────────────────────────────────────────
    # TASK GROUP 1: Data Generation
    # ──────────────────────────────────────────────────────────────────
    with TaskGroup("data_generation", tooltip="Generate synthetic transaction data") as tg_gen:

        generate_transactions = BashOperator(
            task_id="generate_transactions",
            bash_command=(
                "cd /opt/airflow && "
                "python -m data_generator.main generate all "
                "--execution-date '{{ data_interval_start.isoformat() }}'"
            ),
            env={
                "LOCALSTACK_ENDPOINT": os.environ.get("LOCALSTACK_ENDPOINT", "http://localstack:4566"),
                "S3_BUCKET": BUCKET,
                "AWS_ACCESS_KEY_ID": "test",
                "AWS_SECRET_ACCESS_KEY": "test",
                "AWS_DEFAULT_REGION": "us-east-1",
            },
            execution_timeout=timedelta(minutes=15),
        )

    # ──────────────────────────────────────────────────────────────────
    # TASK GROUP 2: Bronze Ingestion
    # ──────────────────────────────────────────────────────────────────
    with TaskGroup("ingestion", tooltip="S3 sensor + Spark Bronze job") as tg_ingest:

        wait_for_s3_data = S3KeySensor(
            task_id="wait_for_s3_data",
            bucket_name=BUCKET,
            bucket_key="raw/transactions/{{ data_interval_start.strftime('%Y/%m/%d/%H') }}/*.jsonl",
            wildcard_match=True,
            aws_conn_id="aws_localstack",
            timeout=600,
            poke_interval=30,
            mode="reschedule",
        )

        spark_bronze_job = SparkSubmitOperator(
            task_id="spark_bronze_job",
            application="/opt/spark/jobs/bronze_ingest.py",
            application_args=[
                "--date", "{{ ds }}",
                "--hour", "{{ data_interval_start.strftime('%H') }}",
                "--bucket", BUCKET,
            ],
            conf=SPARK_CONF,
            conn_id="spark_default",
            env_vars={"PYTHONPATH": "/opt/spark"},
            execution_timeout=timedelta(minutes=20),
            packages=(
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.261,"
                "org.postgresql:postgresql:42.6.0"
            ),
        )

        wait_for_s3_data >> spark_bronze_job

    # ──────────────────────────────────────────────────────────────────
    # TASK GROUP 3: Transformation (Silver + Gold)
    # ──────────────────────────────────────────────────────────────────
    with TaskGroup("transformation", tooltip="Silver cleanse + Gold star schema") as tg_transform:

        spark_silver_job = SparkSubmitOperator(
            task_id="spark_silver_job",
            application="/opt/spark/jobs/silver_cleanse.py",
            application_args=["--date", "{{ ds }}", "--bucket", BUCKET],
            conf=SPARK_CONF,
            conn_id="spark_default",
            env_vars={"PYTHONPATH": "/opt/spark"},
            execution_timeout=timedelta(minutes=25),
            packages=(
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.261,"
                "org.postgresql:postgresql:42.6.0"
            ),
        )

        spark_gold_job = SparkSubmitOperator(
            task_id="spark_gold_job",
            application="/opt/spark/jobs/gold_modeling.py",
            application_args=["--date", "{{ ds }}", "--bucket", BUCKET],
            conf=SPARK_CONF,
            conn_id="spark_default",
            env_vars={"PYTHONPATH": "/opt/spark"},
            execution_timeout=timedelta(minutes=20),
            packages=(
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.261,"
                "org.postgresql:postgresql:42.6.0"
            ),
        )

        spark_silver_job >> spark_gold_job

    # ──────────────────────────────────────────────────────────────────
    # TASK GROUP 4: Quality & dbt Modeling
    # ──────────────────────────────────────────────────────────────────
    with TaskGroup("quality_and_modeling", tooltip="dbt models + Great Expectations") as tg_quality:

        dbt_run = BashOperator(
            task_id="dbt_run",
            bash_command="rm -rf /tmp/dbt_run && cp -a /opt/dbt /tmp/dbt_run && cd /tmp/dbt_run && dbt deps && dbt run --target prod --no-write-json",
            execution_timeout=timedelta(minutes=15),
        )

        dbt_test = BashOperator(
            task_id="dbt_test",
            bash_command="rm -rf /tmp/dbt_test && cp -a /opt/dbt /tmp/dbt_test && cd /tmp/dbt_test && dbt deps && dbt test --target prod",
            execution_timeout=timedelta(minutes=10),
        )

        gx_validate = BashOperator(
            task_id="great_expectations_validate",
            bash_command=(
                "cd /opt/spark && "
                "python -c \"from processing.utils.quality_checks import run_silver_suite; "
                "run_silver_suite('{{ ds }}')\""
            ),
            execution_timeout=timedelta(minutes=10),
        )

        gx_validate >> dbt_run >> dbt_test

    # ──────────────────────────────────────────────────────────────────
    # TASK GROUP 5: Anomaly Detection
    # ──────────────────────────────────────────────────────────────────
    with TaskGroup("anomaly_detection", tooltip="Fraud detection engine + flag update") as tg_anomaly:

        spark_anomaly_detection = SparkSubmitOperator(
            task_id="spark_anomaly_detection",
            application="/opt/spark/jobs/anomaly_detection.py",
            application_args=["--date", "{{ ds }}", "--bucket", BUCKET],
            conf=SPARK_CONF,
            conn_id="spark_default",
            env_vars={"PYTHONPATH": "/opt/spark"},
            execution_timeout=timedelta(minutes=20),
            packages=(
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.261,"
                "org.postgresql:postgresql:42.6.0"
            ),
        )

        update_fraud_flags = PostgresOperator(
            task_id="update_fraud_flags",
            postgres_conn_id="postgres_fintech",
            sql="""
                UPDATE gold.fact_transactions ft
                SET
                    is_flagged_fraud = TRUE,
                    fraud_score      = al.severity_score
                FROM gold.alerts_log al
                WHERE ft.transaction_id = al.transaction_id::uuid
                  AND al.created_at::date = '{{ ds }}'::date
                  AND al.status = 'open';
            """,
        )

        spark_anomaly_detection >> update_fraud_flags

    # ── Pipeline dependency chain ─────────────────────────────────────
    tg_gen >> tg_ingest >> tg_transform >> tg_quality >> tg_anomaly
