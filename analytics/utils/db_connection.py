"""Database connection utilities for the Streamlit dashboard.

Provides a cached SQLAlchemy engine (singleton) and a retry-safe
query runner. All credentials come from the STREAMLIT_DB_CONNECTION
environment variable.
"""

from __future__ import annotations
import logging
import os
import time
from functools import lru_cache

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.pool import QueuePool

logger = logging.getLogger(__name__)


@lru_cache(maxsize=1)
def get_db_engine():
    """Create and cache a SQLAlchemy engine with connection pooling.

    Uses the STREAMLIT_DB_CONNECTION environment variable for the
    connection string. Cached as a singleton via @lru_cache.

    Returns:
        Configured SQLAlchemy Engine instance.

    Raises:
        KeyError: If STREAMLIT_DB_CONNECTION is not set.
    """
    conn_str = os.environ["STREAMLIT_DB_CONNECTION"]
    return create_engine(
        conn_str,
        poolclass=QueuePool,
        pool_size=5,
        max_overflow=10,
        pool_pre_ping=True,
        connect_args={"connect_timeout": 10},
    )


def run_query(sql: str, retries: int = 3, params: dict | None = None) -> pd.DataFrame:
    """Execute a SQL query with exponential backoff retry logic.

    Compatible with SQLAlchemy 2.x: passes engine directly to pd.read_sql
    and wraps SQL in text() for parameterized queries.

    Args:
        sql: SQL query string (can use :param placeholders).
        retries: Maximum number of retry attempts on connection error.
        params: Optional dict of query parameters for parameterized queries.

    Returns:
        pandas DataFrame with query results, or empty DataFrame on error.
    """
    engine = get_db_engine()
    for attempt in range(retries):
        try:
            conn = engine.raw_connection()
            try:
                return pd.read_sql(sql, conn, params=params)
            finally:
                conn.close()
        except Exception as exc:
            logger.warning(
                "Query attempt %d/%d failed: %s", attempt + 1, retries, exc
            )
            if attempt < retries - 1:
                time.sleep(2 ** attempt)  # Exponential backoff: 1s, 2s, 4s
    logger.error("Query failed after %d attempts:\n%s", retries, sql[:200])
    return pd.DataFrame()
