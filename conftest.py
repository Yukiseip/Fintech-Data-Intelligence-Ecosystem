"""Shared pytest fixtures for all test suites."""

from __future__ import annotations
import os
import pytest


def pytest_addoption(parser):
    """Register custom CLI options for pytest."""
    parser.addoption(
        "--run-integration",
        action="store_true",
        default=False,
        help="Run integration tests (requires running Docker stack)",
    )


def pytest_configure(config):
    """Register custom pytest markers."""
    config.addinivalue_line("markers", "integration: mark as integration test")
    config.addinivalue_line("markers", "performance: mark as performance test")
    config.addinivalue_line("markers", "slow: mark as slow test (> 30s)")


def pytest_collection_modifyitems(config, items):
    """Skip integration tests unless --run-integration flag is passed."""
    if not config.getoption("--run-integration"):
        skip_integration = pytest.mark.skip(
            reason="Integration test — add --run-integration to run"
        )
        for item in items:
            if "integration" in item.keywords:
                item.add_marker(skip_integration)


@pytest.fixture(scope="session")
def test_env():
    """Provide test environment variables as a dict.

    Returns:
        Dict of environment variables for test configuration.
    """
    return {
        "S3_BUCKET":           os.environ.get("S3_BUCKET", "fintech-raw-data"),
        "LOCALSTACK_ENDPOINT": os.environ.get("LOCALSTACK_ENDPOINT", "http://localhost:4566"),
        "POSTGRES_HOST":       os.environ.get("POSTGRES_HOST", "localhost"),
        "POSTGRES_PORT":       os.environ.get("POSTGRES_PORT", "5432"),
        "POSTGRES_DB":         os.environ.get("POSTGRES_DB", "fintech_dw"),
        "POSTGRES_USER":       os.environ.get("POSTGRES_USER", "fintech_user"),
        "POSTGRES_PASSWORD":   os.environ.get("POSTGRES_PASSWORD", "CHANGE_ME_BEFORE_USE"),
    }
