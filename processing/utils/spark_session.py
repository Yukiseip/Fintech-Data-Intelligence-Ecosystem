"""Reusable Spark session builder with LocalStack S3A configuration."""

from __future__ import annotations
import os
from pyspark.sql import SparkSession


def get_spark_session(app_name: str, mode: str = "local[*]") -> SparkSession:
    """Create or retrieve a configured SparkSession.

    Configures S3A filesystem to point at LocalStack, adds required JARs
    for S3 and PostgreSQL JDBC access.

    Args:
        app_name: Name of the Spark application (shown in Spark UI).
        mode: Spark master URL or 'local[*]' for unit tests.

    Returns:
        Configured SparkSession instance.
    """
    localstack_endpoint = os.environ.get("LOCALSTACK_ENDPOINT", "http://localstack:4566")
    master_url = os.environ.get("SPARK_MASTER_URL", mode)

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master(master_url)
        # S3A config for LocalStack
        .config("spark.hadoop.fs.s3a.endpoint", localstack_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", "test")
        .config("spark.hadoop.fs.s3a.secret.key", "test")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        # Required JARs
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.261,"
            "org.postgresql:postgresql:42.6.0",
        )
        # Performance tuning
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark
