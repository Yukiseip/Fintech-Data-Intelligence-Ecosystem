"""pytest configuration for processing tests."""

from __future__ import annotations
import os
import pytest

from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """Shared SparkSession for all processing tests.

    Uses local mode — no external cluster needed.
    Configured with minimal resources for unit tests.

    Yields:
        Active local SparkSession.
    """
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("fintech-unit-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.adaptive.enabled", "false")  # Deterministic for tests
        .getOrCreate()
    )
    yield session
    session.stop()
