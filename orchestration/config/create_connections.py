"""Airflow connections bootstrap script.

Run ONCE after `make up` to register:
  - aws_localstack  : S3 connection pointing to LocalStack
  - spark_default   : Spark master for SparkSubmitOperator
  - postgres_fintech: PostgreSQL Gold DWH connection

Usage (auto-run via make up → airflow-init container):
    python orchestration/config/create_connections.py
"""

from __future__ import annotations
import logging
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_connections() -> None:
    """Register all required Airflow connections via the Airflow API.

    Reads all connection params from environment variables to avoid
    hardcoded credentials in code.
    """
    from airflow.models import Connection
    from airflow import settings

    session = settings.Session()

    connections = [
        Connection(
            conn_id="aws_localstack",
            conn_type="aws",
            extra={
                "endpoint_url": os.environ.get("LOCALSTACK_ENDPOINT", "http://localstack:4566"),
                "aws_access_key_id": "test",
                "aws_secret_access_key": "test",
                "region_name": "us-east-1",
            },
        ),
        Connection(
            conn_id="spark_default",
            conn_type="spark",
            host=os.environ.get("SPARK_MASTER_URL", "spark://spark-master:7077"),
            extra={"deploy-mode": "client"},
        ),
        Connection(
            conn_id="postgres_fintech",
            conn_type="postgres",
            host=os.environ.get("POSTGRES_HOST", "postgres"),
            port=int(os.environ.get("POSTGRES_PORT", "5432")),
            schema=os.environ.get("POSTGRES_DB", "fintech_dw"),
            login=os.environ["POSTGRES_USER"],
            password=os.environ["POSTGRES_PASSWORD"],
        ),
    ]

    for conn in connections:
        existing = session.query(Connection).filter_by(conn_id=conn.conn_id).first()
        if existing:
            logger.info("Connection '%s' already exists — skipping.", conn.conn_id)
        else:
            session.add(conn)
            logger.info("Created connection: %s", conn.conn_id)

    session.commit()
    session.close()
    logger.info("All Airflow connections registered successfully.")


if __name__ == "__main__":
    create_connections()
