"""Performance tests validating pipeline SLA targets.

Targets (from TSD):
  O1: Ingest 10K txns/min (Bronze job < 2 min for 100K batch)
  O2: Dashboard queries < 2s p95
  O3: Silver job < 3 min for 100K batch

Run: pytest tests/performance/ -v
"""

from __future__ import annotations
import os
import time
import pytest

import psycopg2


@pytest.fixture(scope="module")
def pg_conn():
    """PostgreSQL connection for performance tests."""
    conn = psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        dbname=os.environ.get("POSTGRES_DB", "fintech_dw"),
        user=os.environ.get("POSTGRES_USER", "fintech_user"),
        password=os.environ.get("POSTGRES_PASSWORD", "CHANGE_ME_BEFORE_USE"),
    )
    yield conn
    conn.close()


def measure_query_time(pg_conn, sql: str, params: dict | None = None) -> float:
    """Return query execution time in seconds.

    Args:
        pg_conn: Active psycopg2 connection.
        sql: SQL query to execute.
        params: Optional query parameters.

    Returns:
        Execution time in seconds as a float.
    """
    cur = pg_conn.cursor()
    start = time.perf_counter()
    cur.execute(sql, params)
    cur.fetchall()
    return time.perf_counter() - start


class TestDashboardQueryLatency:
    """Validate dashboard queries complete within 2s p95 (SLA O3)."""

    SLA_SECONDS = 2.0

    def test_executive_kpis_query_latency(self, pg_conn):
        """TPV + fraud rate aggregate must complete in < 2s."""
        elapsed = measure_query_time(pg_conn, """
            SELECT
                SUM(amount) FILTER (WHERE status = 'completed') AS tpv,
                COUNT(*)                                          AS total_txns,
                AVG(amount) FILTER (WHERE status = 'completed')  AS avg_val,
                ROUND(100.0 * COUNT(*) FILTER (WHERE is_flagged_fraud)
                    / NULLIF(COUNT(*), 0), 4)                    AS fraud_rate_pct
            FROM gold.fact_transactions
            WHERE time_sk >= CAST(TO_CHAR(NOW() - INTERVAL '30 days', 'YYYYMMDD') AS INT)
        """)
        assert elapsed < self.SLA_SECONDS, (
            f"KPI query took {elapsed:.2f}s — exceeds {self.SLA_SECONDS}s SLA"
        )

    def test_tpv_trend_query_latency(self, pg_conn):
        """30-day daily TPV trend query must complete in < 2s."""
        elapsed = measure_query_time(pg_conn, """
            SELECT dt.date, SUM(ft.amount) AS daily_tpv
            FROM gold.fact_transactions ft
            JOIN gold.dim_time dt ON ft.time_sk = dt.time_sk
            WHERE dt.date >= NOW() - INTERVAL '30 days'
              AND ft.status = 'completed'
            GROUP BY dt.date
            ORDER BY dt.date
        """)
        assert elapsed < self.SLA_SECONDS, (
            f"TPV trend query took {elapsed:.2f}s — exceeds {self.SLA_SECONDS}s SLA"
        )

    def test_fraud_alerts_query_latency(self, pg_conn):
        """Recent fraud alerts query must complete in < 2s."""
        elapsed = measure_query_time(pg_conn, """
            SELECT reason_code, COUNT(*) AS count, AVG(severity_score) AS avg_sev
            FROM gold.alerts_log
            WHERE alert_timestamp >= NOW() - INTERVAL '7 days'
            GROUP BY reason_code
        """)
        assert elapsed < self.SLA_SECONDS, (
            f"Alerts query took {elapsed:.2f}s — exceeds {self.SLA_SECONDS}s SLA"
        )
